import csv
import random
from datetime import datetime, timedelta
from faker import Faker
import uuid
import os

fake = Faker()

def generate_transaction(index=None):
    timestamp_initiated = fake.date_time_between(start_date='-30d', end_date='now')
    timestamp_completed = timestamp_initiated + timedelta(seconds=random.randint(1, 120))

    return {
        'transaction_id': str(uuid.uuid4()),
        'account_id': str(uuid.uuid4()),
        'customer_id': str(uuid.uuid4()),
        'merchant_name': fake.company(),
        'store_name': fake.company_suffix(),
        'card_type': random.choice(['VISA', 'MASTERCARD', 'AMEX', 'RUPAY', 'INVALID_CARD']),
        'card_expire_date': fake.date_between(start_date='today', end_date='+5y').strftime('%m/%Y'),
        'transaction_type': random.choice(['ONLINE', 'OFFLINE']),
        'transaction_status': 'FAILED',
        'amount': round(random.uniform(10, 10000), 2),
        'currency': random.choice(['INR', 'USD', 'EUR']),
        'timestamp_initiated': timestamp_initiated.isoformat(),
        'timestamp_completed': timestamp_completed.isoformat(),
        'failure_reason_code': fake.lexify(text='????'),
        'failure_description': fake.sentence(nb_words=6),
        'retry_count': random.randint(0, 3),
        'device_id': str(uuid.uuid4()),
        'ip_address': fake.ipv4_public(),
        'geo_location': f"{fake.latitude():.4f},{fake.longitude():.4f}",
        'created_by': fake.user_name(),
        'created_at': datetime.now().isoformat()
    }

def inject_anomalies(transactions):
    anomalies = []

    for _ in range(10):
        tx = random.choice(transactions)
        dup = tx.copy()
        dup['transaction_id'] = str(uuid.uuid4())
        anomalies.append(dup)

    for _ in range(10):
        tx = generate_transaction(_)
        tx['card_type'] = 'UNKNOWN_ISSUER'
        anomalies.append(tx)

    for _ in range(10):
        tx = generate_transaction(_)
        tx['device_id'] = '0000-INVALID'
        tx['geo_location'] = '0.0000,0.0000'
        anomalies.append(tx)

    for _ in range(10):
        tx = generate_transaction(_)
        tx['retry_count'] = 99
        anomalies.append(tx)

    return anomalies

def save_transactions_to_csv(output_dir="input"):
    os.makedirs(output_dir, exist_ok=True)

    records = [generate_transaction(i) for i in range(960)]
    records += inject_anomalies(records)
    random.shuffle(records)

    output_filename = f"transaction_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    output_path = os.path.join(output_dir, output_filename)

    fieldnames = list(records[0].keys())

    with open(output_path, mode='w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

    print(f"✅ CSV file generated: {output_path} with {len(records)} records")
    return output_path
