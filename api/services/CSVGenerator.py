import random
from datetime import datetime, timedelta,timezone
from faker import Faker
import uuid
import csv
import os


fake = Faker()

# ------------------ Base Transaction ------------------
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

# ------------------ Rules ------------------
def inject_rule_1(data):
    base = generate_base_transaction(True, "duplicate_transaction")
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
    t = generate_base_transaction(True, "voided_but_not_settled")
    t["is_voided"] = True
    t["settlement_status"] = "NOT SETTLED"
    t["settled_amount"] = round(t["amount"] * 0.9, 2)
    t["voided_timestamp"] = (
        datetime.fromisoformat(t["timestamp_completed"]) + timedelta(hours=random.randint(1, 12))
    ).isoformat()
    data.append(t)

def inject_rule_3(data):
    t = generate_base_transaction(True, "late_settlement")
    t["settlement_timestamp"] = (
        datetime.fromisoformat(t["timestamp_completed"]) + timedelta(days=5)
    ).isoformat()
    t["settlement_status"] = "SETTLED"
    data.append(t)

def inject_rule_4(data):
    t = generate_base_transaction(True, "amount_mismatch")
    t["settled_amount"] = round(t["amount"] - (t["amount"] * 0.05), 2)
    data.append(t)

def inject_rule_5(data):
    t = generate_base_transaction(True, "currency_mismatch")
    while t["currency"] == t["terminal_currency"]:
        t["terminal_currency"] = random.choice(["INR", "USD", "EUR"])
    data.append(t)

def inject_hidden_anomalies(data, count):
    for _ in range(count):
        t = generate_base_transaction(True, "hidden_anomaly")
        t["retry_count"] = random.randint(10, 20)
        t["transaction_status"] = "FAILED"
        t["timestamp_completed"] = t["timestamp_initiated"]
        t["settlement_timestamp"] = (
            datetime.fromisoformat(t["timestamp_completed"]) - timedelta(days=random.randint(1, 2))
        ).isoformat()
        t["banking_charge"] = round(t["amount"] * 0.1, 2)
        data.append(t)

# ------------------ Generator ------------------
def generate_dataset(output_dir="input"):
    os.makedirs(output_dir, exist_ok=True)
    num_records = 100
    anomaly_ratio = 0.6
    num_anomalies = int(num_records * anomaly_ratio)
    num_normals = num_records - num_anomalies

    data = []

    # Normal records
    for _ in range(num_normals):
        data.append(generate_base_transaction(False))

    # Anomalies using rules
    injectors = [inject_rule_1, inject_rule_2, inject_rule_3, inject_rule_4, inject_rule_5]
    while len([d for d in data if d["anomaly_type"] != "NORMAL"]) < num_anomalies:
        rule = random.choice(injectors + [lambda d: inject_hidden_anomalies(d, 1)])
        rule(data)

    # Shuffle & limit to exact count
    random.shuffle(data)
    data = data[:num_records]

    # Save CSV
    output_filename = f"transaction_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    filepath = os.path.join(output_dir, output_filename)

    with open(filepath, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=list(data[0].keys()))
        writer.writeheader()
        writer.writerows(data)

    print(f"✅ Dataset generated: {filepath} | Records: {len(data)} | Anomalies: {len([d for d in data if d['anomaly_type'] != 'NORMAL'])}")
    return filepath
