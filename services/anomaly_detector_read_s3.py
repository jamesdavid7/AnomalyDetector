import os
import json
import tempfile
import pandas as pd
import ipaddress
from datetime import datetime
from geopy.distance import geodesic
from flask import Flask, request, jsonify
from sklearn.ensemble import IsolationForest, RandomForestClassifier
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
import boto3

app = Flask(__name__)

REFERENCE_GEO = (12.9716, 77.5946)

def preprocess(df):
    df['timestamp_initiated'] = pd.to_datetime(df['timestamp_initiated'])
    df['timestamp_completed'] = pd.to_datetime(df['timestamp_completed'])
    df['duration_sec'] = (df['timestamp_completed'] - df['timestamp_initiated']).dt.total_seconds()
    df['hour'] = df['timestamp_initiated'].dt.hour
    df['day_of_week'] = df['timestamp_initiated'].dt.dayofweek
    df['months_to_expiry'] = df['card_expire_date'].apply(
        lambda x: (pd.to_datetime(x, format='%m/%Y').year - pd.Timestamp.now().year) * 12 +
                  (pd.to_datetime(x, format='%m/%Y').month - pd.Timestamp.now().month)
    )
    df['geo_distance_km'] = df['geo_location'].apply(
        lambda loc: geodesic(REFERENCE_GEO, tuple(map(float, loc.split(',')))).km
    )
    return df

def detect_rule_anomalies(row):
    anomalies = []
    if row['amount'] > 5000:
        anomalies.append("HIGH_AMOUNT")
    if row['duration_sec'] > 300:
        anomalies.append("LONG_DURATION")
    if row['hour'] in range(0, 5):
        anomalies.append("ODD_HOUR")
    if row['months_to_expiry'] < 1:
        anomalies.append("CARD_EXPIRY_SOON")
    if row['geo_distance_km'] > 1000:
        anomalies.append("GEO_TOO_FAR")
    try:
        if ipaddress.ip_address(row['ip_address']).is_private:
            anomalies.append("PRIVATE_IP")
    except:
        anomalies.append("IP_PARSE_ERROR")
    if row['transaction_status'] != "SUCCESS":
        anomalies.append("STATUS_NOT_SUCCESS")
    return anomalies

def process_csv_from_s3(bucket, key):
    s3 = boto3.client("s3")

    # Download file from S3
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        s3.download_file(bucket, key, tmp.name)
        df = pd.read_csv(tmp.name)

    df = preprocess(df)
    df['rule_anomalies'] = df.apply(lambda row: detect_rule_anomalies(row), axis=1)

    df['has_high_amount'] = df['amount'] > 5000
    df['has_long_duration'] = df['duration_sec'] > 300
    df['has_odd_hour'] = df['hour'].isin(range(0, 5))
    df['has_expiring_card'] = df['months_to_expiry'] < 1
    df['has_geo_far'] = df['geo_distance_km'] > 1000
    df['has_status_fail'] = df['transaction_status'] != 'SUCCESS'
    df['is_anomaly_suspected_supervised'] = df['rule_anomalies'].apply(lambda x: len(x) >= 3)

    features_unsup = ['amount', 'duration_sec', 'hour', 'day_of_week', 'months_to_expiry', 'geo_distance_km']
    X_unsup = df[features_unsup].fillna(0)
    X_unsup_scaled = StandardScaler().fit_transform(X_unsup)

    iso_model = IsolationForest(contamination=0.02, random_state=42)
    df['iso_anomaly'] = iso_model.fit_predict(X_unsup_scaled)
    df['iso_score'] = iso_model.decision_function(X_unsup_scaled)
    df['iso_score'] = MinMaxScaler(feature_range=(1, 100)).fit_transform(df[['iso_score']])

    features_sup = features_unsup + [
        'retry_count', 'has_high_amount', 'has_long_duration', 'has_odd_hour',
        'has_expiring_card', 'has_geo_far', 'has_status_fail'
    ]
    X = df[features_sup].astype(float)
    y = df['is_anomaly_suspected_supervised']

    if y.value_counts().get(True, 0) < 100:
        from sklearn.utils import resample
        df_majority = df[df['is_anomaly_suspected_supervised'] == False]
        df_minority = df[df['is_anomaly_suspected_supervised'] == True]
        df_minority_upsampled = resample(df_minority, replace=True, n_samples=150, random_state=42)
        df = pd.concat([df_majority, df_minority_upsampled])
        X = df[features_sup].astype(float)
        y = df['is_anomaly_suspected_supervised']

    X_train, X_test, y_train, y_test = train_test_split(X, y, stratify=y, test_size=0.3, random_state=42)
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    rf_model = RandomForestClassifier(n_estimators=200, class_weight='balanced', random_state=42)
    rf_model.fit(X_train_scaled, y_train)
    df['is_anomaly_suspected_UnSupervised'] = rf_model.predict(scaler.transform(X))

    print("✅ Supervised Model Report:")
    print(classification_report(y_test, rf_model.predict(X_test_scaled)))

    # Save and upload result
    output_file = os.path.join(tempfile.gettempdir(), "transactions_with_anomalies.csv")
    df.to_csv(output_file, index=False)

    output_key = "output/transactions_with_anomalies.csv"
    s3.upload_file(output_file, bucket, output_key)
    print(f"✅ Uploaded to s3://{bucket}/{output_key}")

    return output_key


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
