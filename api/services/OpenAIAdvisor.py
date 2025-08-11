import openai
import pandas as pd
import json
import time
from dotenv import load_dotenv
import os

start_time = time.time()  # ⏱ Start timing
load_dotenv()
# Set your API key (recommended: use environment variables instead)
openai.api_key = os.getenv("OPENAI_API_KEY")

# # Load data from CSV
# BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# #input_path = os.path.join(BASE_DIR, "output", "transactions_with_anomalies.csv")
#
# input_path = "output\\transactions_with_anomalies.csv"  # adjust path if needed
# df = pd.read_csv(input_path).head(1)

# Function to analyze transaction using OpenAI o4-mini or gpt-4o
def analyze_transaction(row):
    prompt = f"""
    Analyze the following POS transaction and determine whether it exhibits any anomalies. If an anomaly is detected, classify it (e.g., high refund, unusual card usage, transaction mismatch) including from the below mentioned anomaly types accordingly, explain the reasoning behind the classification, and recommend an appropriate action. If the transaction appears normal, state that clearly.
    Anomaly types:
    Duplicate Transactions
    Reversed/Voided Transactions Not Settled
    Delayed Settlement
    Mismatch in Captured vs. Settled Amount
    Card Type/Issuer Anomalies
    Terminal Location Inconsistency
    Offline Transactions (Fallbacks)
    Terminal Configuration Errors
    Recurring Declines for Same Card
    MDR (Merchant Discount Rate) Mismatches
    Unusual Refund Frequency
    Unbalanced Batch Totals
    Operator-Level Fraud or Errors
    Multiple Settlement Batches in a Day
    Decline Code Pattern Analysis
    
    Please note that our internal LLM-based system, which uses an Isolation Forest algorithm, has already evaluated the transaction and produced the following fields:
    is_anomaly_suspected_supervised: indicating the supervised model’s prediction
    is_anomaly_suspected_unsupervised: indicating the unsupervised models's prediction


    Use this information in conjunction with your analysis to generate a reliable fraud evaluation response suitable for downstream review or audit.
    
    Transaction:
    - Transaction ID: {row['transaction_id']}
    - Account ID: {row['account_id']}
    - Customer ID: {row['customer_id']}
    - Merchant: {row['merchant_name']}
    - Store: {row['store_name']}
    - Card Type: {row['card_type']}
    - Card Expire Date: {row['card_expire_date']}
    - Transaction Type: {row['transaction_type']}
    - Transaction Status: {row['transaction_status']}
    - Amount: {row['amount']}
    - Currency: {row['currency']}
    - Timestamp Initiated: {row['timestamp_initiated']}
    - Timestamp Completed: {row['timestamp_completed']}
    - Retry Count: {row['retry_count']}
    - Device ID: {row['device_id']}
    - IP ADDRESS: {row['ip_address']}
    - Geo Location: {row['geo_location']}
    - Created By: {row['created_by']}
    - Created At: {row['created_at']}
    - IS Anomaly suspected supervised: {row['is_anomaly_suspected_supervised']}
    - IS Anomaly suspected Unsupervised: {row['is_anomaly_suspected_unsupervised']}
    - Rule Anomalies: {row['rule_anomalies']}
    
    Respond in this JSON format:
    {{
      "anomaly": true/false,
      "anomaly_type":"..",
      "classification": "...",
      "explanation": "...",
      "suggested_action": "...",
      "anomaly_score": float (between 0 and 100, where 100 = highly anomalous)
    }}
    """

    try:
        response = openai.chat.completions.create(
            model="o4-mini",
            messages=[{"role": "user", "content": prompt}]
        )
        content = response.choices[0].message.content
        result = json.loads(content)
        # ⏱ End timing and print elapsed time
        end_time = time.time()
        elapsed = end_time - start_time
        # to: {output_path}
        print(f"✅ Analysis complete. Results saved ")
        print(f"🕒 Time taken: {elapsed:.2f} seconds")
        return pd.Series([
            result.get("anomaly"),
            result.get("anomaly_type"),
            result.get("classification"),
            result.get("explanation"),
            result.get("suggested_action"),
            result.get("anomaly_score")
        ])
    except Exception as e:
        print(f"Error on {row['transaction_id']}: {e}")
        return pd.Series([None, None, None, None, None, None])

# # Apply to all rows (optional: limit to 10 rows for cost/testing)
# output_columns = ["open_ai_anomaly", "anomaly_type", "classification", "explanation", "suggested_action", "anomaly_score"]
# df[output_columns] = df.apply(analyze_transaction, axis=1)
#
# # Save enriched results
# output_dir = "output"
# filename = "from_openai.csv"
#
# # Create the output directory if it doesn't exist
# os.makedirs(output_dir, exist_ok=True)
#
# output_path = os.path.join(output_dir, filename)
# df.to_csv(output_path, index=False)


