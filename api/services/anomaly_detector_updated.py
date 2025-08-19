import os
import random
import tempfile
from collections import Counter
from datetime import datetime, timezone, timedelta

import boto3
import pandas as pd
# Your imports for metric & OpenAI advisory
from api.config.constatns import TABLE_ANOMALY_METRICS
from api.dynamodb.metric_data import MetricDataRepo
from api.models.metric import Metric
from api.services.OpenAIAdvisor import analyze_transaction
from flask import Flask
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import MinMaxScaler
import shap


app = Flask(__name__)

REFERENCE_GEO = (12.9716, 77.5946)

def detect_rule_anomalies(row):
    anomalies = []
    if row['anomaly_type'] != "NORMAL":
        return row['anomaly_type']  # Known injected anomaly type
    if row['has_high_amount']: anomalies.append("high_amount")
    if row['has_long_duration']: anomalies.append("long_duration")
    if row['has_odd_hour']: anomalies.append("odd_hour")
    if row['has_expiring_card']: anomalies.append("card_expiring_soon")
    if row['has_geo_far']: anomalies.append("geo_far")
    if row['retry_count'] > 10: anomalies.append('high_retry_count')
    if row['transaction_duration'] < 0.5: anomalies.append('instant_transaction')
    if row['charge_percent'] > 0.05: anomalies.append('unusually_high_banking_charge')
    if row['settlement_delay_days'] < 0: anomalies.append('pre_settlement')
    if row['amount_per_minute'] > 100: anomalies.append('high_amount_per_minute')

    return random.choice(anomalies) if anomalies else "none"
def process_csv_from_s3(bucket, key):
    s3 = boto3.client("s3")

    # Download file from S3
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        temp_file_path = tmp.name

    s3.download_file(bucket, key, temp_file_path)
    df = pd.read_csv(temp_file_path)

    df['has_high_amount'] = df['amount'] > 5000
    df['has_long_duration'] = (
            pd.to_datetime(df['timestamp_completed']) - pd.to_datetime(df['timestamp_initiated'])
    ).dt.total_seconds().apply(lambda x: x > 3600)
    df['has_odd_hour'] = pd.to_datetime(df['timestamp_initiated']).dt.hour.apply(lambda x: x < 5)
    current_time = datetime.now().replace(tzinfo=None)
    df['has_expiring_card'] = df['card_expire_date'].apply(
        lambda x: datetime.strptime(x, "%m/%Y") < (current_time + timedelta(days=30))
    )
    df['has_geo_far'] = df['geo_location'].apply(lambda x: random.random() < 0.05)
    # === Extra features for Isolation Forest ===
    df['transaction_duration'] = (
                                         pd.to_datetime(df['timestamp_completed']) - pd.to_datetime(
                                     df['timestamp_initiated'])
                                 ).dt.total_seconds() / 60

    df['settlement_delay_days'] = (
                                          pd.to_datetime(df['settlement_timestamp']) - pd.to_datetime(
                                      df['timestamp_completed'])
                                  ).dt.total_seconds() / (3600 * 24)

    df['currency_mismatch_flag'] = (df['currency'] != df['terminal_currency']).astype(int)
    df['status_fail_flag'] = (df['transaction_status'] == 'FAILED').astype(int)
    df['is_voided'] = df['is_voided'].astype(int)
    df['charge_percent'] = df['banking_charge'] / df['amount']
    df['amount_per_minute'] = df['amount'] / (df['transaction_duration'] + 0.1)
    df['rule_anomalies'] = df.apply(lambda row: detect_rule_anomalies(row), axis=1)
    df['is_anomaly_suspected_supervised'] = (df['anomaly_type'] != "NORMAL")

    features = df[[
        'amount', 'banking_charge', 'settled_amount', 'retry_count',
        'transaction_duration', 'settlement_delay_days', 'is_voided',
        'currency_mismatch_flag', 'status_fail_flag', 'charge_percent', 'amount_per_minute'
    ]].fillna(0)

    # === Isolation Forest ===
    iso_model = IsolationForest(contamination=0.25, n_estimators=300, random_state=42)
    predictions = iso_model.fit_predict(features)
    df['iso_anomaly'] = pd.Series(predictions, index=df.index).map({-1: True, 1: False})
    df['iso_score_raw'] = iso_model.decision_function(features)

    scaler = MinMaxScaler(feature_range=(1, 100))
    df['iso_score'] = scaler.fit_transform(df[['iso_score_raw']])
    df.drop(columns=['iso_score_raw'], inplace=True)
    df['is_anomaly_suspected_unsupervised'] = df['iso_anomaly']
    # === SHAP Explainability for Isolation Forest ===
    explainer = shap.Explainer(iso_model, features)
    shap_values = explainer(features)

    def get_iso_reason(row_idx, top_n=2):
        """Return top N features influencing anomaly decision."""
        shap_row = shap_values.values[row_idx]
        top_features_idx = abs(shap_row).argsort()[::-1][:top_n]
        return [features.columns[i] for i in top_features_idx]

    df['iso_anomaly_reason'] = [
        get_iso_reason(i) if df.loc[i, 'iso_anomaly'] else ['normal']
        for i in range(len(df))
    ]
    output_columns = [
        'transaction_id', 'account_id', 'customer_id', 'merchant_name', 'store_name',
        'card_type', 'card_expire_date', 'transaction_type', 'transaction_status', 'amount', 'currency',
        'timestamp_initiated', 'timestamp_completed', 'settlement_status', 'settlement_timestamp',
        'has_high_amount', 'has_long_duration', 'has_odd_hour',
        'has_expiring_card', 'has_geo_far', 'voided_timestamp',
        'is_anomaly_suspected_supervised', 'is_anomaly_suspected_unsupervised',
        'iso_anomaly', 'iso_score', 'rule_anomalies', 'iso_anomaly_reason'
    ]

    # Save and upload result
    output_file = os.path.join(tempfile.gettempdir(), "transactions_with_anomalies.csv")
    df[output_columns].to_csv(output_file, index=False)

    # Apply OpenAI LLM Analysis on first 10 records
    df_to_analyze = df.head(12).copy()
    advisory_output_cols = ["open_ai_anomaly", "anomaly_type", "classification", "explanation", "suggested_action",
                            "anomaly_score"]
    df_to_analyze[advisory_output_cols] = df_to_analyze.apply(analyze_transaction, axis=1)

    # Get current timestamp in YYYYMMDD_HHMMSS format
    timestamp = datetime.now(timezone.utc).replace(microsecond=0).strftime("%Y-%m-%dT%H-%M-%S")

    filename = f"transactions_with_anomalies_{timestamp}.csv"
    output_key = f"output/{filename}"

    advisory_output_file = os.path.join(tempfile.gettempdir(), filename)
    df_to_analyze.to_csv(advisory_output_file, index=False)
    print(f"🔍 OpenAI advisory saved at: {advisory_output_file}")

    # Count the frequency of each anomaly type
    counts = Counter(df_to_analyze['anomaly_type'])

    # Construct the metric_data list
    metric_data = [
        {"anomaly_type": k, "count": v}
        for k, v in counts.items()
    ]

    # Final dictionary
    metric_dict = {
        "file_name": filename,
        "metric_data": metric_data
    }

    # sending metrics to dynamodb
    metric = Metric.to_metric(metric_dict)
    db = MetricDataRepo(TABLE_ANOMALY_METRICS)
    db.insert_item(metric)

    s3.upload_file(advisory_output_file, bucket, output_key)
    print(f"✅ Uploaded to s3://{bucket}/{output_key}")

    return output_key


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
