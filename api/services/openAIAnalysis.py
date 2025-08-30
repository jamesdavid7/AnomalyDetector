import openai
import json
import time
from dotenv import load_dotenv
import os

start_time = time.time()  # ⏱ Start timing
load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")


# Function to analyze transaction JSON using OpenAI
def analyze_transaction(transaction: dict) -> dict:
    prompt = f"""
    Analyze the following POS transaction and determine whether it exhibits any anomalies. 
    If an anomaly is detected, classify it (e.g., high refund, unusual card usage, transaction mismatch) 
    including from the below mentioned anomaly types, explain the reasoning, and recommend an appropriate action. 
    If the transaction appears normal, state that clearly.

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

    Transaction JSON:
    {json.dumps(transaction, indent=2)}

    Respond ONLY in this JSON format:
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
            model= "gpt-4.1-nano",
            #"o4-mini, gpt-4.1-nano",
            messages=[{"role": "user", "content": prompt}]
        )
        content = response.choices[0].message.content
        result = json.loads(content)

        # Merge anomaly results into transaction JSON
        updated_transaction = {**transaction, **{
            "is_anomaly": result.get("anomaly", False),
            "anomaly_type": result.get("anomaly_type"),
            "classification": result.get("classification"),
            "explanation": result.get("explanation"),
            "suggested_action": result.get("suggested_action"),
            "anomaly_score": result.get("anomaly_score", 0.0)
        }}

        # ⏱ Timing info
        end_time = time.time()
        elapsed = end_time - start_time
        print(f"✅ Analysis complete. Time taken: {elapsed:.2f} seconds")

        return updated_transaction

    except Exception as e:
        print(f"❌ Error analyzing transaction {transaction.get('transaction_id')}: {e}")
        return {**transaction, "analysis_error": str(e)}
