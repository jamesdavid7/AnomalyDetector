import csv
import random
import uuid
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()

# Parameters
TOTAL_RECORDS = 1000
ANOMALY_PER_RULE = 10
RULES = [1, 2, 3, 4, 5]
normal_records = []
anomalies = []


def generate_base_transaction():
    timestamp_initiated = fake.date_time_between(start_date='-30d', end_date='now')
    timestamp_completed = timestamp_initiated + timedelta(minutes=random.randint(1, 60))
    amount = round(random.uniform(100, 5000), 2)
    banking_charge = round(amount * 0.01, 2)
    settled_amount = round(amount - banking_charge, 2)
    settlement_timestamp = timestamp_completed + timedelta(days=random.randint(0, 2))
    settlement_status = random.choice(["SETTLED", "PENDING"])
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
        'transaction_status': random.choice(['SUCCESS', 'FAILED']),
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
        'created_at': datetime.now().isoformat()
    }


# Generate base data
for _ in range(TOTAL_RECORDS - ANOMALY_PER_RULE * len(RULES)):
    normal_records.append(generate_base_transaction())


# Inject anomalies per rule
def inject_rule_1(data):
    """Duplicate transaction anomaly"""
    base = generate_base_transaction()
    duplicate = base.copy()
    duplicate['transaction_id'] = str(uuid.uuid4())
    data.extend([base, duplicate])


def inject_rule_2(data):
    t = generate_base_transaction()
    t['is_voided'] = True
    t['settlement_status'] = 'SETTLED'
    t['settled_amount'] = round(t['amount'] * 0.9, 2)
    t['voided_timestamp'] = (
            datetime.fromisoformat(t['timestamp_completed']) +
            timedelta(hours=random.randint(1, 12))
    ).isoformat()
    data.append(t)

def inject_rule_3(data):
    t = generate_base_transaction()
    t['settlement_timestamp'] = (datetime.fromisoformat(t['timestamp_completed']) + timedelta(days=5)).isoformat()
    data.append(t)


def inject_rule_4(data):
    t = generate_base_transaction()
    t['settled_amount'] = round(t['amount'] - (t['amount'] * 0.05), 2)  # 5% deduction instead of 1%
    data.append(t)


def inject_rule_5(data):
    t = generate_base_transaction()
    while t['currency'] == t['terminal_currency']:
        t['terminal_currency'] = random.choice(['INR', 'USD', 'EUR'])
    data.append(t)


# Create anomalies
for _ in range(ANOMALY_PER_RULE):
    inject_rule_1(anomalies)
    inject_rule_2(anomalies)
    inject_rule_3(anomalies)
    inject_rule_4(anomalies)
    inject_rule_5(anomalies)

# Combine all
final_data = normal_records + anomalies

# Write to CSV
with open('synthetic_transactions.csv', 'w', newline='', encoding='utf-8') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=final_data[0].keys())
    writer.writeheader()
    writer.writerows(final_data)

print("✅ 1k records generated with anomalies in synthetic_transactions.csv")
