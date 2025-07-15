import csv
import random
from datetime import datetime, timedelta
from faker import Faker

# Initialize faker and seed for consistency
fake = Faker()
Faker.seed(0)
random.seed(0)

# CSV Header
headers = [
    "transactionId", "transactionType", "transactionDate", "transactionStatus",
    "merchantName", "storeName", "deviceName", "location", "totalAmount",
    "cardHolderName", "cardType", "currencyCode", "failureReason"
]

# Sample values
transaction_types = ["PURCHASE", "REFUND", "CASHBACK"]
card_types = ["VISA", "MASTERCARD", "AMEX", "RUPAY"]
currency_codes = ["USD", "EUR", "INR", "GBP"]

# Generate one fake record
def generate_transaction_record(i):
    return {
        "transactionId": f"TXN{i:06}",
        "transactionType": random.choice(transaction_types),
        "transactionDate": (datetime.now() - timedelta(days=random.randint(0, 365))).strftime("%Y-%m-%d %H:%M:%S"),
        "merchantName": fake.company(),
        "storeName": fake.company_suffix(),
        "deviceName": f"POS-{random.randint(1000, 9999)}",
        "location": fake.city(),
        "totalAmount": round(random.uniform(10.0, 1000.0), 2),
        "cardHolderName": fake.name(),
        "cardType": random.choice(card_types),
        "currencyCode": random.choice(currency_codes)
    }

# Generate 1000 records
records = [generate_transaction_record(i) for i in range(1, 1001)]

# Write to CSV
output_file = "card_transactions_with_reasons.csv"
with open(output_file, mode="w", newline="", encoding="utf-8") as file:
    writer = csv.DictWriter(file, fieldnames=headers)
    writer.writeheader()
    writer.writerows(records)

print(f"CSV file '{output_file}' generated successfully with 1000 records.")
