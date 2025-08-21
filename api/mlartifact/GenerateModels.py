import uuid
import random
from datetime import datetime, timedelta
import pandas as pd
from faker import Faker
from sklearn.ensemble import RandomForestClassifier, IsolationForest
from sklearn.preprocessing import StandardScaler
import joblib

# ----------------------------------------
# Setup
# ----------------------------------------
fake = Faker()

# ----------------------------------------
# Generate Base Transaction
# ----------------------------------------
def generate_base_transaction(anomaly=False):
    timestamp_initiated = fake.date_time_between(start_date='-30d', end_date='now')
    duration_min = random.randint(1, 120)
    timestamp_completed = timestamp_initiated + timedelta(minutes=duration_min)
    amount = round(random.uniform(100, 5000), 2)
    banking_charge = round(amount * 0.01, 2)

    # Inject anomaly if requested
    if anomaly:
        amount *= random.uniform(5, 10)
        banking_charge *= random.uniform(5, 10)
        duration_min *= random.uniform(2, 5)
        timestamp_completed = timestamp_initiated + timedelta(minutes=duration_min)

    return {
        'transaction_id': str(uuid.uuid4()),
        'account_id': str(uuid.uuid4()),
        'customer_id': str(uuid.uuid4()),
        'merchant_name': fake.company(),
        'store_name': fake.company_suffix(),
        'card_number': fake.credit_card_number(),
        'card_type': random.choice(['VISA', 'MASTERCARD', 'AMEX', 'RUPAY']),
        'card_expire_date': fake.date_between(start_date='today', end_date='+5y').strftime('%m/%Y'),
        'amount': amount,
        'currency': random.choice(['INR', 'USD', 'EUR']),
        'terminal_currency': random.choice(['INR', 'USD', 'EUR']),
        'timestamp_initiated': timestamp_initiated.isoformat(),
        'timestamp_completed': timestamp_completed.isoformat(),
        'device_id': str(uuid.uuid4()),
        'ip_address': fake.ipv4_public(),
        'geo_location': f"{fake.latitude():.4f},{fake.longitude():.4f}",
        'banking_charge': banking_charge,
        'created_by': fake.user_name(),
        'created_at': datetime.now().isoformat(),
        'is_anomaly': anomaly
    }

# ----------------------------------------
# Generate Training Data
# ----------------------------------------
transactions = [generate_base_transaction() for _ in range(480)]
transactions += [generate_base_transaction(anomaly=True) for _ in range(20)]
df = pd.DataFrame(transactions)

# ----------------------------------------
# Encode categorical fields
# ----------------------------------------
cat_cols = ['card_type','currency','terminal_currency','merchant_name','store_name']
encoder = {}
for col in cat_cols:
    df[col] = df[col].astype('category')
    encoder[col] = dict(zip(df[col].cat.categories, range(len(df[col].cat.categories))))
    df[col+'_code'] = df[col].apply(lambda x: int(encoder[col].get(x, -1)))

# ----------------------------------------
# Convert timestamps and compute duration
# ----------------------------------------
df['timestamp_initiated'] = pd.to_datetime(df['timestamp_initiated'], errors='coerce').fillna(pd.Timestamp.now())
df['timestamp_completed'] = pd.to_datetime(df['timestamp_completed'], errors='coerce').fillna(pd.Timestamp.now())

df['timestamp_initiated_epoch'] = df['timestamp_initiated'].apply(lambda x: int(x.timestamp()))
df['timestamp_completed_epoch'] = df['timestamp_completed'].apply(lambda x: int(x.timestamp()))
df['transaction_duration'] = (df['timestamp_completed_epoch'] - df['timestamp_initiated_epoch']) / 60

# ----------------------------------------
# Prepare ML features
# ----------------------------------------
feature_cols = [
    'amount', 'banking_charge', 'transaction_duration',
    'timestamp_initiated_epoch', 'timestamp_completed_epoch',
    'card_type_code', 'currency_code', 'terminal_currency_code'
]

features = df[feature_cols].fillna(0)
labels = df['is_anomaly'].astype(int)

# ----------------------------------------
# Compute training stats for anomaly reasoning
# ----------------------------------------
training_stats = {}
for col in feature_cols:
    series = pd.Series(features[col]).astype(float)  # ensure Series
    training_stats[col] = {
        'mean': series.mean(),
        'std': series.std()
    }

joblib.dump(training_stats, "training_stats.pkl")
print("✅ training_stats.pkl created successfully")

# ----------------------------------------
# Scale numeric features for Isolation Forest
# ----------------------------------------
scaler = StandardScaler()
features_scaled = scaler.fit_transform(features)
joblib.dump(scaler, "scaler.pkl")

# ----------------------------------------
# Train Random Forest
# ----------------------------------------
rf_model = RandomForestClassifier(n_estimators=200, random_state=42)
rf_model.fit(features, labels)
joblib.dump(rf_model, "random_forest_model_all_params.pkl")

# ----------------------------------------
# Train Isolation Forest
# ----------------------------------------
iso_model = IsolationForest(contamination=0.05, n_estimators=100, random_state=42)
iso_model.fit(features_scaled)
joblib.dump(iso_model, "isolation_forest_model_all_params.pkl")

# ----------------------------------------
# Save categorical encoder
# ----------------------------------------
joblib.dump(encoder, "categorical_encoder.pkl")

print("✅ Models and artifacts saved successfully.")
