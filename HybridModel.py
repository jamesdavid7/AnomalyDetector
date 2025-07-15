import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import LabelEncoder

# Load your data
df = pd.read_csv("card_transactions_with_reasons.csv")

# Drop any existing status columns to simulate unlabeled case
df.drop(columns=["transactionStatus", "failureReason"], inplace=True, errors="ignore")

# --------- STEP 1: RULE-BASED FAILURES ----------
def rule_based_flags(row):
    if row["totalAmount"] > 5000 and row["cardType"] == "RUPAY":
        return "FAILED", "Card not supported"
    elif row["cardType"] == "AMEX" and row["currencyCode"] == "INR":
        return "FAILED", "Currency mismatch"
    elif "POS-9999" in row["deviceName"]:
        return "FAILED", "Terminal misconfig"
    else:
        return "UNKNOWN", "UNKNOWN"

df[["transactionStatus", "failureReason"]] = df.apply(rule_based_flags, axis=1, result_type="expand")

# --------- STEP 2: UNSUPERVISED OUTLIER DETECTION ----------

# Encode categorical fields for modeling
df_encoded = df.copy()
categorical_cols = ["transactionType", "merchantName", "storeName", "deviceName", "location", "cardHolderName", "cardType", "currencyCode"]

encoders = {}
for col in categorical_cols:
    le = LabelEncoder()
    df_encoded[col] = le.fit_transform(df_encoded[col])
    encoders[col] = le

# Select numerical features for anomaly detection
feature_cols = ["transactionType", "totalAmount", "cardType", "deviceName", "currencyCode"]
X = df_encoded[feature_cols]

# Apply Isolation Forest
model = IsolationForest(contamination=0.1, random_state=42)
df["anomalyScore"] = model.fit_predict(X)  # -1 is anomaly

# --------- STEP 3: Update UNKNOWNs with AI-anomaly reason ----------
def update_by_model(row):
    if row["transactionStatus"] == "UNKNOWN" and row["anomalyScore"] == -1:
        return "FAILED", "Suspicious transaction (anomaly)"
    elif row["transactionStatus"] == "UNKNOWN":
        return "SUCCESS", "SUCCESS"
    else:
        return row["transactionStatus"], row["failureReason"]

df[["transactionStatus", "failureReason"]] = df.apply(update_by_model, axis=1, result_type="expand")

# Drop helper column
df.drop(columns=["anomalyScore"], inplace=True)

# Save to file
df.to_csv("transactions_enriched_unsupervised.csv", index=False)
print("✅ Updated CSV generated with rule+unsupervised model:")
