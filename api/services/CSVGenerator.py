import random
from datetime import datetime, timedelta
from faker import Faker
import uuid
import csv
import os

fake = Faker()

# ------------------ Base Transaction ------------------
def generate_base_transaction(is_anomaly=False, anomaly_type="NORMAL"):
    timestamp_initiated = fake.date_time_between(start_date='-30d', end_date='now')
    timestamp_completed = timestamp_initiated + timedelta(minutes=random.randint(1, 60))

    return {
        "transaction_id": str(uuid.uuid4()),
        "card_number": fake.credit_card_number(),
        "amount": round(random.uniform(100, 5000), 2),
        "currency": random.choice(["INR", "USD", "EUR"]),
        "terminal_currency": random.choice(["INR", "USD", "EUR"]),
        "device_id": str(uuid.uuid4()),
        "merchant_name": fake.company(),
        "transaction_status": random.choice(["SUCCESS", "FAILED"]),
        "is_voided": False,
        "settlement_status": "SETTLED",
        "settled_amount": None,
        "timestamp_initiated": timestamp_initiated.isoformat(),
        "timestamp_completed": timestamp_completed.isoformat(),
        "settlement_timestamp": (
            timestamp_completed + timedelta(hours=random.randint(1, 48))
        ).isoformat(),
        "retry_count": random.randint(0, 3),
        "banking_charge": round(random.uniform(1, 100), 2),
        "anomaly_type": anomaly_type if is_anomaly else "NORMAL",
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
def generate_dataset(num_records=100, anomaly_ratio=0.6, output_dir="input"):
    os.makedirs(output_dir, exist_ok=True)

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
