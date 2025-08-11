import os
import random
import uuid
from datetime import datetime, timedelta, timezone

import pandas as pd
from faker import Faker
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import MinMaxScaler
from collections import Counter

# Your imports for metric & OpenAI advisory
from api.config.constatns import TABLE_ANOMALY_METRICS
from api.dynamodb.metric_data import MetricDataRepo
from api.models.metric import Metric
from api.services.OpenAIAdvisor import analyze_transaction

fake = Faker()

# === Parameters ===
TOTAL_RECORDS = 100
ANOMALY_PERCENT = 0.7
ANOMALY_COUNT = int(TOTAL_RECORDS * ANOMALY_PERCENT)  # 70 anomaly records
NORMAL_COUNT = TOTAL_RECORDS - ANOMALY_COUNT          # 30 normal records
HIDDEN_ANOMALY_COUNT = int(ANOMALY_COUNT * 0.1)       # 10% hidden subtle anomalies ~7

RULES = ["duplicate_transaction", "voided_but_not_settled", "late_settlement", "amount_mismatch", "currency_mismatch"]

normal_records = []
anomalies = []

# === Base Transaction Generator ===
def generate_base_transaction(label=False, anomaly_type="NORMAL"):
    timestamp_initiated = fake.date_time_between(start_date='-30d', end_date='now')
    duration_min = random.randint(1, 120)
    timestamp_completed = timestamp_initiated + timedelta(minutes=duration_min)
    amount = round(random.uniform(100, 5000), 2)
    banking_charge = round(amount * 0.01, 2)
    settlement_status = random.choice(["SETTLED", "NOT SETTLED"])

    if settlement_status == "SETTLED":
        settled_amount = round(amount - banking_charge, 2)
        settlement_timestamp = timestamp_completed + timedelta(days=random.randint(0, 2))
    else:
        settled_amount = 0.0
        settlement_timestamp = None

    is_voided = False

    return {
        'transaction_id': str(uuid.uuid4()),
        'account_id': str(uuid.uuid4()),
        'customer_id': str(uuid.uuid4()),
        'merchant_name': fake.company(),
        'store_name': fake.company_suffix(),
        'card_number': fake.credit_card_number(card_type=None),
        'card_type': random.choice(['VISA', 'MASTERCARD', 'AMEX', 'RUPAY']),
        'card_expire_date': fake.date_between(start_date='today', end_date='+5y').strftime('%m/%Y'),
        'transaction_type': random.choice(['ONLINE', 'OFFLINE']),
        'transaction_status': "FAILED",
        'amount': amount,
        'currency': random.choice(['INR', 'USD', 'EUR']),
        'terminal_currency': random.choice(['INR', 'USD', 'EUR']),
        'timestamp_initiated': timestamp_initiated.isoformat(),
        'timestamp_completed': timestamp_completed.isoformat(),
        'failure_reason_code': fake.lexify(text='????'),
        'failure_description': fake.sentence(nb_words=6),
        'retry_count': random.randint(0, 3),
        'device_id': str(uuid.uuid4()),
        'ip_address': fake.ipv4_public(),
        'geo_location': f"{fake.latitude():.4f},{fake.longitude():.4f}",
        'is_voided': is_voided,
        'settlement_status': settlement_status,
        'voided_timestamp': '',
        'banking_charge': banking_charge,
        'settled_amount': settled_amount,
        'settlement_timestamp': settlement_timestamp.isoformat() if settlement_timestamp else '',
        'created_by': fake.user_name(),
        'created_at': datetime.now(timezone.utc).isoformat(),
        'is_anomaly': label,
        'anomaly_type': anomaly_type
    }

# === Anomaly Injection Functions ===

def inject_rule_1(data):
    # Duplicate transaction anomaly: add original + duplicate (same key fields)
    base = generate_base_transaction(True, 'duplicate_transaction')
    duplicate = base.copy()
    # Force same fields for duplication
    duplicate['card_number'] = base['card_number']
    duplicate['amount'] = base['amount']
    duplicate['device_id'] = base['device_id']
    duplicate['merchant_name'] = base['merchant_name']
    duplicate['timestamp_completed'] = base['timestamp_completed']
    data.append(base)
    data.append(duplicate)

def inject_rule_2(data):
    # Voided but not settled
    t = generate_base_transaction(True, 'voided_but_not_settled')
    t['is_voided'] = True
    t['settlement_status'] = 'NOT SETTLED'
    t['settled_amount'] = round(t['amount'] * 0.9, 2)
    t['voided_timestamp'] = (
        datetime.fromisoformat(t['timestamp_completed']) +
        timedelta(hours=random.randint(1, 12))
    ).isoformat()
    data.append(t)

def inject_rule_3(data):
    # Late settlement (more than expected delay)
    t = generate_base_transaction(True, 'late_settlement')
    t['settlement_timestamp'] = (
        datetime.fromisoformat(t['timestamp_completed']) + timedelta(days=5)
    ).isoformat()
    t['settlement_status'] = 'SETTLED'
    data.append(t)

def inject_rule_4(data):
    # Amount mismatch between transaction and settled amount
    t = generate_base_transaction(True, 'amount_mismatch')
    t['settled_amount'] = round(t['amount'] - (t['amount'] * 0.05), 2)
    data.append(t)

def inject_rule_5(data):
    # Currency mismatch between transaction currency and terminal currency
    t = generate_base_transaction(True, 'currency_mismatch')
    while t['currency'] == t['terminal_currency']:
        t['terminal_currency'] = random.choice(['INR', 'USD', 'EUR'])
    data.append(t)

def inject_hidden_anomalies(data, count):
    # Subtle anomalies that Isolation Forest may catch
    for _ in range(count):
        t = generate_base_transaction(False, 'NORMAL')
        t['retry_count'] = random.randint(10, 20)
        t['transaction_status'] = "FAILED"
        t['timestamp_completed'] = t['timestamp_initiated']  # zero duration
        t['settlement_timestamp'] = (
            datetime.fromisoformat(t['timestamp_completed']) - timedelta(days=random.randint(1, 2))
        ).isoformat()  # settlement before transaction date
        t['banking_charge'] = round(t['amount'] * 0.1, 2)  # high banking charge
        data.append(t)

# === Generate dataset ===

# 1. Normal records (30)
for _ in range(NORMAL_COUNT):
    normal_records.append(generate_base_transaction(label=False, anomaly_type="NORMAL"))

# 2. Hidden subtle anomalies (10)
inject_hidden_anomalies(anomalies, HIDDEN_ANOMALY_COUNT)

# 3. Rule-based anomalies (60) distributed evenly among 5 rules
rule_anomaly_count = ANOMALY_COUNT - HIDDEN_ANOMALY_COUNT
rule_injections_each = rule_anomaly_count // len(RULES)  # 12 each approx

for _ in range(rule_injections_each):
    inject_rule_1(anomalies)
    inject_rule_2(anomalies)
    inject_rule_3(anomalies)
    inject_rule_4(anomalies)
    inject_rule_5(anomalies)

# === Combine normal + anomalies ===
df = pd.DataFrame(normal_records + anomalies)

# === Heuristic feature engineering ===
current_time = datetime.now(timezone.utc)
df['has_high_amount'] = df['amount'].apply(lambda x: x > 4000)
df['has_long_duration'] = (
    pd.to_datetime(df['timestamp_completed']) - pd.to_datetime(df['timestamp_initiated'])
).dt.total_seconds().apply(lambda x: x > 3600)
df['has_odd_hour'] = pd.to_datetime(df['timestamp_initiated']).dt.hour.apply(lambda x: x < 5)
current_time = datetime.now().replace(tzinfo=None)
df['has_expiring_card'] = df['card_expire_date'].apply(
    lambda x: datetime.strptime(x, "%m/%Y") < (current_time + timedelta(days=30))
)
df['has_geo_far'] = df['geo_location'].apply(lambda x: random.random() < 0.05)

def detect_rule_anomalies(row):
    anomalies = []
    if row['anomaly_type'] != "NORMAL":
        return row['anomaly_type']  # Known injected anomaly type
    if row['has_high_amount']: anomalies.append("high_amount")
    if row['has_long_duration']: anomalies.append("long_duration")
    if row['has_odd_hour']: anomalies.append("odd_hour")
    if row['has_expiring_card']: anomalies.append("card_expiring_soon")
    if row['has_geo_far']: anomalies.append("geo_far")
    return random.choice(anomalies) if anomalies else "none"

df['rule_anomalies'] = df.apply(detect_rule_anomalies, axis=1)

# Label for injected anomalies (supervised)
df['is_anomaly_suspected_supervised'] = (df['anomaly_type'] != "NORMAL")

# === Extra features for Isolation Forest ===
df['transaction_duration'] = (
    pd.to_datetime(df['timestamp_completed']) - pd.to_datetime(df['timestamp_initiated'])
).dt.total_seconds() / 60

df['settlement_delay_days'] = (
    pd.to_datetime(df['settlement_timestamp']) - pd.to_datetime(df['timestamp_completed'])
).dt.total_seconds() / (3600 * 24)

df['currency_mismatch_flag'] = (df['currency'] != df['terminal_currency']).astype(int)
df['status_fail_flag'] = (df['transaction_status'] == 'FAILED').astype(int)
df['is_voided'] = df['is_voided'].astype(int)
df['charge_percent'] = df['banking_charge'] / df['amount']
df['amount_per_minute'] = df['amount'] / (df['transaction_duration'] + 0.1)

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

def get_unsupervised_reason(row):
    reasons = []
    if row['retry_count'] > 10: reasons.append('high_retry_count')
    if row['transaction_duration'] < 0.5: reasons.append('instant_transaction')
    if row['charge_percent'] > 0.05: reasons.append('unusually_high_banking_charge')
    if row['settlement_delay_days'] < 0: reasons.append('pre_settlement')
    if row['amount_per_minute'] > 100: reasons.append('high_amount_per_minute')
    return random.choice(reasons) if reasons else 'normal'

df['iso_anomaly_reason'] = df.apply(
    lambda row: get_unsupervised_reason(row) if row['iso_anomaly'] else 'normal', axis=1)

# === Save full dataset CSV ===
final_csv = 'synthetic_transactions_processed_v2.csv'
df.to_csv(final_csv, index=False)
print(f"✅ Final dataset saved: {final_csv}")

# === OpenAI advisory on first 10 records ===
output_dir = r"../output"
os.makedirs(output_dir, exist_ok=True)

df_to_analyze = df.head(10).copy()
advisory_output_cols = ["open_ai_anomaly", "anomaly_type", "classification", "explanation", "suggested_action", "anomaly_score"]
def safe_analyze(row):
    try:
        result = analyze_transaction(row)
        if not result or len(result) != 6:
            return [None]*6
        return result
    except Exception as e:
        print(f"Error analyzing row {row.name}: {e}")
        return [None]*6
# Make sure analyze_transaction returns a list/Series with 6 elements matching advisory_output_cols
df_to_analyze[advisory_output_cols] = df_to_analyze.apply(analyze_transaction, axis=1, result_type='expand')

timestamp = datetime.now(timezone.utc).replace(microsecond=0).strftime("%Y-%m-%dT%H-%M-%S")
filename = f"transactions_with_anomalies_{timestamp}.csv"
advisory_output_file = os.path.join(output_dir, filename)

df_to_analyze.to_csv(advisory_output_file, index=False)
print(f"🔍 OpenAI advisory saved at: {advisory_output_file}")

# === Metrics to DynamoDB ===
counts = Counter(df_to_analyze['anomaly_type'])
metric_data = [{"anomaly_type": k, "count": v} for k, v in counts.items()]
metric_dict = {
    "file_name": filename,
    "metric_data": metric_data
}

metric = Metric.to_metric(metric_dict)
db = MetricDataRepo(TABLE_ANOMALY_METRICS)

try:
    db.insert_item(metric)
    print("✅ Metrics inserted successfully.")
except Exception as e:
    print(f"❌ Failed to insert metrics: {e}")

# === Summary ===
print(f"Total records: {len(df)}")
print(f"Injected anomalies (supervised): {df['is_anomaly_suspected_supervised'].sum()}")
print(f"Isolation Forest detected anomalies (unsupervised): {df['is_anomaly_suspected_unsupervised'].sum()}")
