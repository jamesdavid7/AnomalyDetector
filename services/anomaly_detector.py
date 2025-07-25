import pandas as pd
import random
import ipaddress
from faker import Faker
from datetime import datetime, timedelta
from geopy.distance import geodesic
from sklearn.ensemble import IsolationForest, RandomForestClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
import os
from sklearn.preprocessing import MinMaxScaler


def generate_and_process_data():
    fake = Faker()
    num_records = 1000
    REFERENCE_GEO = (12.9716, 77.5946)

    data = []
    for _ in range(num_records):
        timestamp_initiated = fake.date_time_between(start_date='-1y', end_date='now')
        timestamp_completed = timestamp_initiated + timedelta(seconds=random.randint(10, 300))

        row = {
            'transaction_id': fake.uuid4(),
            'account_id': fake.uuid4(),
            'customer_id': fake.uuid4(),
            'merchant_name': fake.company(),
            'store_name': fake.company_suffix(),
            'card_type': random.choice(['VISA', 'MASTERCARD', 'AMEX', 'RUPAY']),
            'card_expire_date': fake.date_between(start_date='today', end_date='+5y').strftime('%m/%Y'),
            'transaction_type': random.choice(['ONLINE', 'OFFLINE']),
            'transaction_status': 'FAILED',
            'amount': round(random.uniform(10, 10000), 2),
            'currency': random.choice(['INR', 'USD', 'EUR']),
            'timestamp_initiated': timestamp_initiated,
            'timestamp_completed': timestamp_completed,
            'failure_reason_code': fake.lexify(text='????'),
            'failure_description': fake.sentence(nb_words=6),
            'retry_count': random.randint(0, 3),
            'device_id': fake.uuid4(),
            'ip_address': fake.ipv4_public(),
            'geo_location': f"{fake.latitude():.4f},{fake.longitude():.4f}",
            'created_by': fake.user_name(),
            'created_at': datetime.now().isoformat()
        }
        data.append(row)

    df = pd.DataFrame(data)

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

    df = preprocess(df)

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

    # Apply anomaly detection
    df['rule_anomalies'] = df.apply(lambda row: detect_rule_anomalies(row), axis=1)

    # Rule-based features
    df['has_high_amount'] = df['amount'] > 5000
    df['has_long_duration'] = df['duration_sec'] > 300
    df['has_odd_hour'] = df['hour'].isin(range(0, 5))
    df['has_expiring_card'] = df['months_to_expiry'] < 1
    df['has_geo_far'] = df['geo_distance_km'] > 1000
    df['has_status_fail'] = df['transaction_status'] != 'SUCCESS'

    # Supervised label
    df['is_anomaly_suspected_supervised'] = df['rule_anomalies'].apply(lambda x: len(x) >= 3)

    # Unsupervised Isolation Forest
    features_unsup = ['amount', 'duration_sec', 'hour', 'day_of_week', 'months_to_expiry', 'geo_distance_km']
    X_unsup = df[features_unsup].fillna(0)
    X_unsup_scaled = StandardScaler().fit_transform(X_unsup)

    iso_model = IsolationForest(contamination=0.02, random_state=42)
    df['iso_anomaly'] = iso_model.fit_predict(X_unsup_scaled)
    df['iso_score'] = iso_model.decision_function(X_unsup_scaled)
    # Rescale iso_score to 1-100
    scaler_iso = MinMaxScaler(feature_range=(1, 100))
    df['iso_score'] = scaler_iso.fit_transform(df[['iso_score']])

    # Supervised RandomForest
    features_sup = features_unsup + [
        'retry_count', 'has_high_amount', 'has_long_duration', 'has_odd_hour',
        'has_expiring_card', 'has_geo_far', 'has_status_fail'
    ]
    X = df[features_sup].astype(float)
    y = df['is_anomaly_suspected_supervised']

    # Upsample if needed
    if y.value_counts()[True] < 100:
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

    # Arrange final output columns
    output_columns = [
        'transaction_id', 'account_id', 'customer_id', 'merchant_name', 'store_name',
        'card_type', 'card_expire_date', 'transaction_type', 'transaction_status', 'amount', 'currency',
        'timestamp_initiated', 'timestamp_completed', 'duration_sec', 'hour', 'day_of_week',
        'months_to_expiry', 'geo_location', 'geo_distance_km', 'ip_address', 'retry_count', 'device_id',
        'created_by', 'created_at',
        'has_high_amount', 'has_long_duration', 'has_odd_hour',
        'has_expiring_card', 'has_geo_far', 'has_status_fail',
        'is_anomaly_suspected_supervised', 'is_anomaly_suspected_UnSupervised',
        'iso_anomaly', 'iso_score', 'rule_anomalies'
    ]

    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, "transactions_with_anomalies.csv")
    df[output_columns].to_csv(output_file, index=False)
    return os.path.abspath(output_file)



# Run and generate file
if __name__ == "__main__":
    path = generate_and_process_data()
    print(f"CSV saved at: {path}")
