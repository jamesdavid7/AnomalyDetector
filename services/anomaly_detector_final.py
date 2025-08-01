import csv
import random
import uuid
import pandas as pd
from datetime import datetime, timedelta
from faker import Faker
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import MinMaxScaler

fake = Faker()

# Parameters
TOTAL_RECORDS = 1000
ANOMALY_PERCENT = 0.20
ANOMALY_COUNT = int(TOTAL_RECORDS * ANOMALY_PERCENT)
HIDDEN_ANOMALY_COUNT = int(TOTAL_RECORDS * 0.02)
RULES = ["duplicate_transaction", "voided_but_settled", "late_settlement", "amount_mismatch", "currency_mismatch"]

normal_records = []
anomalies = []

def generate_base_transaction(label=False, anomaly_type="NORMAL"):
    timestamp_initiated = fake.date_time_between(start_date='-30d', end_date='now')
    duration_min = random.randint(1, 120)
    timestamp_completed = timestamp_initiated + timedelta(minutes=duration_min)
    amount = round(random.uniform(100, 5000), 2)
    banking_charge = round(amount * 0.01, 2)
    settled_amount = round(amount - banking_charge, 2)
    settlement_timestamp = timestamp_completed + timedelta(days=random.randint(0, 2))
    settlement_status = random.choice(["SETTLED", "NOT SETTLED"])
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
        'settlement_timestamp': settlement_timestamp.isoformat(),
        'created_by': fake.user_name(),
        'created_at': datetime.now().isoformat(),
        'is_anomaly': label,
        'anomaly_type': anomaly_type
    }

# Inject rule-based anomalies
def inject_rule_1(data):
    base = generate_base_transaction(True, 'duplicate_transaction')
    duplicate = base.copy()
    duplicate['transaction_id'] = str(uuid.uuid4())
    data.append(duplicate)

def inject_rule_2(data):
    t = generate_base_transaction(True, 'voided_but_settled')
    t['is_voided'] = True
    t['settlement_status'] = 'SETTLED'
    t['settled_amount'] = round(t['amount'] * 0.9, 2)
    t['voided_timestamp'] = (
        datetime.fromisoformat(t['timestamp_completed']) +
        timedelta(hours=random.randint(1, 12))
    ).isoformat()
    data.append(t)

def inject_rule_3(data):
    t = generate_base_transaction(True, 'late_settlement')
    t['settlement_timestamp'] = (datetime.fromisoformat(t['timestamp_completed']) + timedelta(days=5)).isoformat()
    data.append(t)

def inject_rule_4(data):
    t = generate_base_transaction(True, 'amount_mismatch')
    t['settled_amount'] = round(t['amount'] - (t['amount'] * 0.05), 2)
    data.append(t)

def inject_rule_5(data):
    t = generate_base_transaction(True, 'currency_mismatch')
    while t['currency'] == t['terminal_currency']:
        t['terminal_currency'] = random.choice(['INR', 'USD', 'EUR'])
    data.append(t)

# Inject subtle hidden anomalies for Isolation Forest to find
def inject_hidden_anomalies(data, count):
    for _ in range(count):
        t = generate_base_transaction(False, 'NORMAL')
        t['retry_count'] = random.randint(10, 20)
        t['transaction_status'] = "FAILED"
        t['timestamp_completed'] = t['timestamp_initiated']  # 0 duration
        t['settlement_timestamp'] = (
            datetime.fromisoformat(t['timestamp_completed']) - timedelta(days=random.randint(1, 2))
        ).isoformat()  # settlement before transaction
        t['banking_charge'] = round(t['amount'] * 0.1, 2)  # very high charge
        data.append(t)

# Normal records
for _ in range(TOTAL_RECORDS - ANOMALY_COUNT):
    normal_records.append(generate_base_transaction())

inject_hidden_anomalies(normal_records, HIDDEN_ANOMALY_COUNT)

# Inject rule anomalies
for _ in range(ANOMALY_COUNT // len(RULES)):
    inject_rule_1(anomalies)
    inject_rule_2(anomalies)
    inject_rule_3(anomalies)
    inject_rule_4(anomalies)
    inject_rule_5(anomalies)

# Combine
df = pd.DataFrame(normal_records + anomalies)

# Heuristic features
current_time = datetime.now()
df['has_high_amount'] = df['amount'].apply(lambda x: x > 4000)
df['has_long_duration'] = (
    pd.to_datetime(df['timestamp_completed']) - pd.to_datetime(df['timestamp_initiated'])
).dt.total_seconds().apply(lambda x: x > 3600)
df['has_odd_hour'] = pd.to_datetime(df['timestamp_initiated']).dt.hour.apply(lambda x: x < 5)
df['has_expiring_card'] = df['card_expire_date'].apply(
    lambda x: datetime.strptime(x, "%m/%Y") < (current_time + timedelta(days=30))
)
df['has_geo_far'] = df['geo_location'].apply(lambda x: random.random() < 0.05)

def detect_rule_anomalies(row):
    anomalies = []
    if row['anomaly_type'] != "NORMAL":
        return row['anomaly_type']
    if row['has_high_amount']: anomalies.append("high_amount")
    if row['has_long_duration']: anomalies.append("long_duration")
    if row['has_odd_hour']: anomalies.append("odd_hour")
    if row['has_expiring_card']: anomalies.append("card_expiring_soon")
    if row['has_geo_far']: anomalies.append("geo_far")
    return random.choice(anomalies) if anomalies else "none"

df['rule_anomalies'] = df.apply(detect_rule_anomalies, axis=1)

# Supervised label: only injected anomalies
df['is_anomaly_suspected_supervised'] = (df['anomaly_type'] != "NORMAL")

# Extra features for Isolation Forest
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

# Isolation Forest
iso_model = IsolationForest(contamination=0.25, n_estimators=300, random_state=42)
predictions = iso_model.fit_predict(features)
df['iso_anomaly'] = pd.Series(predictions, index=df.index).map({-1: True, 1: False})
df['iso_score_raw'] = iso_model.decision_function(features)
scaler = MinMaxScaler(feature_range=(1, 100))
df['iso_score'] = scaler.fit_transform(df[['iso_score_raw']])
df.drop(columns=['iso_score_raw'], inplace=True)
df['is_anomaly_suspected_UnSupervised'] = df['iso_anomaly']

# Isolation Forest anomaly reason
def get_unsupervised_reason(row):
    reasons = []
    if row['retry_count'] > 10: reasons.append('high_retry_count')
    if row['transaction_duration'] < 0.5: reasons.append('instant_transaction')
    if row['charge_percent'] > 0.05: reasons.append('unusually_high_banking_charge')
    if row['settlement_delay_days'] < 0: reasons.append('pre_settlement')
    if row['amount_per_minute'] > 100: reasons.append('high_amount_per_minute')
    return random.choice(reasons) if reasons else 'normal'

df['iso_anomaly_reason'] = df.apply(lambda row: get_unsupervised_reason(row) if row['iso_anomaly'] else 'normal', axis=1)

# Save
final_csv = 'synthetic_transactions_processed_v2.csv'
df.to_csv(final_csv, index=False)
print("✅ Final dataset saved:", final_csv)
