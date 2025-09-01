# backend.py
import json
import os
import traceback
from collections import Counter
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from dateutil.parser import parser
#
os.environ["EVENTLET_NO_GREENDNS"] = "yes"
import eventlet
from api.services.openAIAnalysis import analyze_transaction

eventlet.monkey_patch(os=False, thread=False, subprocess=False)
from threading import Thread

import joblib
import pandas as pd
from api.dynamodb.anomaly_transaction_repo import AnomalyTransactionRepository
from api.models.anomaly_transaction import AnomalyTransaction
from api.utils import ses_utils
from config.constatns import S3_BUCKET_NAME, PROCESSED_DATA_DIR, TABLE_ANOMALY_METRICS, INPUT_DATA_DIR, \
    TABLE_ANOMALY_TRANSACTION, TABLE_BATCH_ANOMALY_TRANSACTION
from dynamodb.metric_data import MetricDataRepo
from flask import Flask, jsonify, request
from flask import send_file
from flask_socketio import SocketIO
from models.metric import Metric
from services.CSVGenerator import generate_dataset
from services.anomaly_detector import generate_and_process_data
# from services.anomaly_detector_read_s3 import process_csv_from_s3  # <-- adjust if needed
from services.anomaly_detector_updated import process_csv_from_s3
from services.anomaly_rules import anomaly_rules
from services.csv_generation import save_transactions_to_csv
from utils.s3_utils import S3Utils
from api.dynamodb.batch_anomaly_transaction_repo import BatchAnomalyTransactionRepository
from api.models.batch_anomaly_transaction import BatchAnomalyTransaction
from api.utils.common_utils import safe_to_epoch, to_dt_utc

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*")  # allow Streamlit to connect

OUTPUT_FOLDER = "output"
OUTPUT_FILENAME = "transactions_with_anomalies.csv"
# Load models once (for performance)
rf_model = joblib.load("mlartifact/random_forest_model_all_params.pkl")
iso_model = joblib.load("mlartifact/isolation_forest_model_all_params.pkl")
encoder = joblib.load("mlartifact/categorical_encoder.pkl")
scaler = joblib.load("mlartifact/scaler.pkl")
training_stats = joblib.load("mlartifact/training_stats.pkl")  # dict with mean/std per column


numeric_cols = [
    'amount', 'banking_charge', 'transaction_duration'
]
cat_cols = ['card_type', 'currency', 'terminal_currency']

feature_cols = numeric_cols + [c + "_code" for c in cat_cols]

@app.route('/run-anomaly-detection', methods=['GET'])
def run_detection():
    output_path = generate_and_process_data()
    s3_utils = S3Utils(bucket_name=S3_BUCKET_NAME)
    filename = s3_utils.send_file_to_s3(output_path, PROCESSED_DATA_DIR)
    ses_utils.process_and_send_file(output_path)
    return jsonify({
        "file_name": filename,
        "message": "Anomaly report generated successfully."
    })
@app.route('/files/upload', methods=['GET'])
def uploadFile():
    output_path = save_transactions_to_csv()
    s3_utils = S3Utils(bucket_name=S3_BUCKET_NAME)
    filename = s3_utils.send_file_to_s3(output_path, INPUT_DATA_DIR)
    return jsonify({
        "file_name": filename,
        "message": "Input file uploaded successfully."
    })

@app.route('/anomalies/import', methods=['GET'])
def uploadFileS3():
    output_path = generate_dataset()
    s3_utils = S3Utils(bucket_name=S3_BUCKET_NAME)
    filename = s3_utils.send_file_to_s3(output_path, INPUT_DATA_DIR)
    return jsonify({
        "file_name": filename,
        "message": "Input file uploaded successfully."
    })

@app.route('/download/<file_name>', methods=['GET'])
def download_file(file_name):
    s3_utils = S3Utils(bucket_name=S3_BUCKET_NAME)
    s3_path = f"{PROCESSED_DATA_DIR}/{file_name}"
    file_stream = s3_utils.download_file_data(s3_path)
    return send_file(
        file_stream,
        mimetype='application/octet-stream',
        as_attachment=True,
        download_name=file_name
    )

@app.route('/metrics', methods=['GET'])
def get_all_metrics():
    db_utils = MetricDataRepo(TABLE_ANOMALY_METRICS)
    return db_utils.get_all_items()

@app.route('/metrics/<metric_id>', methods=['GET'])
def get_metric_by_id(metric_id):
    db_utils = MetricDataRepo(TABLE_ANOMALY_METRICS)
    key = {"metric_id": metric_id}
    return db_utils.get_item(key)

@app.route('/metrics', methods=['POST'])
def create_metric():
    try:
        data = request.get_json()
        metric = Metric.to_metric(data)
        db = MetricDataRepo(TABLE_ANOMALY_METRICS)
        db.insert_item(metric)

        return jsonify(metric), 201

    except Exception as e:
        return jsonify({"error": str(e)}), 400

"""
@app.route("/files/processAnomaly", methods=["POST"])
def process_anomaly():
    print("Inside this method process from lamda to reach this api cal")
    try:
        data = request.get_json(force=True)
        bucket = data["bucket"]
        key = data["key"]
        result_key = process_csv_from_s3(bucket, key)
        return jsonify({"status": "success", "processed_key": result_key}), 200
    except Exception as e:
        print("Error:", str(e))
        return jsonify({"status": "error", "message": str(e)}), 500
"""

@app.route("/files/processAnomaly", methods=["POST"])
def process_anomaly():
    print("Inside this method process from lamda to reach this api cal")
    try:
        data = request.get_json(force=True)
        bucket = data["bucket"]
        key = data["key"]
        result_key = process_csv_from_s3(bucket, key)
        return jsonify({"status": "success", "processed_key": result_key}), 200
    except Exception as e:
        print("Error:", str(e))
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/files/processAnomaly/bg", methods=["POST"])
def process_anomaly_bg():
    print("Inside this method process from lamda to reach this api cal bg")
    try:
        data = request.get_json(force=True)
        bucket = data["bucket"]
        key = data["key"]
        thread = Thread(target=process_csv_from_s3, args=(bucket, key))
        thread.start()
        return "Task started", 202
    except Exception as e:
        print("Error:", str(e))
        return jsonify({"status": "error", "message": str(e)}), 500

# ----------------------------------------
# Utility to prepare features
def prepare_features(txn,encoder):
    df = pd.DataFrame([txn])

    # Encode categorical columns
    for col in cat_cols:
        df[col] = df[col].astype(str)
        df[col + "_code"] = df[col].apply(lambda x: int(encoder[col].get(x, -1)))

    # ensure all exist
    for col in feature_cols:
        if col not in df:
            df[col] = -1

    return df[feature_cols].fillna(0)


def feature_anomaly_reason(txn, features):
    reasons = []
    # Numeric outliers (3-sigma rule)
    for col in numeric_cols:
        val = features[col].iloc[0]
        if val is None:
            continue  # skip if value is missing
        mean, std = training_stats[col]['mean'], training_stats[col]['std']
        if std > 0 and abs(val - mean) > 3*std:
            reasons.append(f"{col}={val:.2f} (outlier)")

    # Categorical unseen
    for col in cat_cols:
        val = txn.get(col)
        if val is None:
            continue
        if val not in encoder[col]:
            reasons.append(f"{col}='{val}' (unseen category)")

        # Only join if there are reasons
    return "; ".join(reasons) if reasons else None
anomalies = []
@app.route("/transactions/detect-anomaly", methods=["POST"])
def detect_single_anomaly():

    try:
        txn = request.get_json(force=True)
        detections = []
        for rule in anomaly_rules:
            before = set(d["anomaly_type"] for d in txn.get("detections", []))
            txn = rule(txn)
            after = set(d["anomaly_type"] for d in txn.get("detections", []))
            new_anomalies = after - before
            for anomaly_type in new_anomalies:
                detections.append({
                    "reason": anomaly_type
                })

        features = prepare_features(txn,encoder)
        features_scaled = scaler.transform(features)

        rf_pred = rf_model.predict(features)[0]
        rf_proba = rf_model.predict_proba(features)[0]
        rf_score = float(rf_proba[1]) if len(rf_proba) > 1 else 0.0

        iso_pred = iso_model.predict(features_scaled)[0]
        iso_score = -iso_model.score_samples(features_scaled)[0]

        model_detections = []
        if rf_pred == 1:
            reason = feature_anomaly_reason(txn, features)
            model_detections.append(("ML_RF", rf_score, reason))
        if iso_pred == -1:
            reason = feature_anomaly_reason(txn, features)
            model_detections.append(("ML_ISO", iso_score, reason))

        if model_detections:
            best_model = max(model_detections, key=lambda x: x[1])
            if best_model[2]:
                detections.append({
                "reason": best_model[2]
            })

            # Build transaction
            transaction = AnomalyTransaction(
                transaction_id=txn.get("transaction_id"),
                account_id=txn.get("account_id"),
                customer_name=txn.get("customer_name"),
                customer_id=txn.get("customer_id"),
                merchant_name=txn.get("merchant_name"),
                store_name=txn.get("store_name"),
                card_number=txn.get("card_number"),
                customer_location=txn.get("customer_location"),
                card_type=txn.get("card_type"),
                card_expire_date=txn.get("card_expire_date"),
                transaction_type=txn.get("transaction_type"),
                transaction_amount=float(txn.get("amount", 0)),
                transaction_status=txn.get("transaction_status"),
                banking_charge=float(txn.get("banking_charge", 0)),
                currency=txn.get("currency"),
                terminal_currency=txn.get("terminal_currency"),
                terminal_id=txn.get("terminal_id"),
                timestamp_initiated=safe_to_epoch(txn.get("timestamp_initiated")),
                timestamp_completed=safe_to_epoch(txn.get("timestamp_completed")),
                retry_count=int(txn.get("retry_count", 0)),
                device_id=txn.get("device_id"),
                ip_address=txn.get("ip_address"),
                geo_location=txn.get("geo_location"),
                created_by=txn.get("created_by"),
                created_at=safe_to_epoch(txn.get("created_at")),
                entry_mode=txn.get("entry_mode"),
                is_anomaly=len(detections) > 0,
                detections=detections
            )

            # ✅ If anomaly detected → call OpenAI to enrich details
            if transaction.is_anomaly:
                print("➡️ Sending anomaly for enrichment via OpenAI...")
                enriched_txn = analyze_transaction(transaction.to_item(transaction))
                # Update transaction with enriched anomaly fields
                transaction.anomaly_type = enriched_txn.get("anomaly_type")
                transaction.classification = enriched_txn.get("classification")
                transaction.explanation = enriched_txn.get("explanation")
                transaction.suggested_action = enriched_txn.get("suggested_action")
                transaction.anomaly_score = enriched_txn.get("anomaly_score", 0.0)

                #Emit anomaly event
                socketio.emit(
                    'anomaly_detected',
                    {
                        "transaction_id": transaction.transaction_id,
                        "customer_name": transaction.customer_name,
                        "amount": transaction.transaction_amount
                    },
                )
                print(f"🚨 Anomaly detected and enriched: {transaction.transaction_id}")

            # ✅ Store *all* transactions (normal + anomaly)
            db = AnomalyTransactionRepository(TABLE_ANOMALY_TRANSACTION)
            print("final txn : ", transaction)
            db.save(transaction)
            print(f"✅ Transaction {transaction.transaction_id} saved to DynamoDB")

            return jsonify(transaction.__dict__), 200

    except Exception as e:
        print(traceback.format_exc())
        return jsonify({
            "error": str(e),
            "trace": traceback.format_exc()
        }), 500

@app.route('/anomaly_transaction', methods=['GET'])
def get_all_anomaly_transactions():
    try:
        trans_repo = AnomalyTransactionRepository(TABLE_ANOMALY_TRANSACTION)
        return trans_repo.get_all_items()
    except Exception as e:
        app.logger.error(str(e))
        return jsonify({"error": str(e)}), 500

@app.route('/anomaly_transaction/id/<transaction_id>', methods=['GET'])
def get_anomaly_transaction_by_id(transaction_id):
    try:
        trans_repo = AnomalyTransactionRepository(TABLE_ANOMALY_TRANSACTION)
        txn = trans_repo.scan_by_txn_id(transaction_id)  # scan method
        if not txn:
            return jsonify({"message": "Transaction not found"}), 404
        return jsonify(txn), 200
    except Exception as e:
        print(traceback.format_exc())
        app.logger.error(str(e))
        return jsonify({"error": str(e)}), 500

@app.get("/anomaly_transaction/real_time_counters")
def get_anomaly_transaction_stats():
    try:
        repo = AnomalyTransactionRepository(TABLE_ANOMALY_TRANSACTION)
        current_time = datetime.utcnow()
        now_epoch = int(current_time.timestamp())

        # 1️⃣ High value transactions > 2000 in last 15 minutes
        high_value_cutoff = int((current_time - timedelta(minutes=15)).timestamp() * 1000)
        high_value = repo.scan_with_filters({
            "transaction_amount": {"gt": 2000},
            "timestamp_initiated": {"gt": high_value_cutoff}
        })

        # 2️⃣ Refunds in last 10 minutes
        refunds_cutoff = int((current_time - timedelta(minutes=10)).timestamp())
        refunds_last_10 = repo.scan_with_filters({
            "transaction_status": {"eq": "REFUND"},
            "timestamp_initiated": {"gt": refunds_cutoff}
        })

        # 3️⃣ Transactions outside business hours (0-6 AM)
        all_transactions = repo.get_all_items()  # No other way to filter by hour
        outside_hours_count = 0
        manual_entries_count = 0
        recent_1h_count = 0
        manual_ratio = 0.0

        for txn in all_transactions:

            ts = txn.timestamp_initiated
            txn_dt = safe_datetime(ts)
            if ts:
                if 0 <= txn_dt.hour <= 6:
                    outside_hours_count += 1
                # 4️⃣ Manual entry ratio in last 1 hour
                if txn_dt > current_time - timedelta(hours=1):
                    recent_1h_count += 1
                    if txn.entry_mode == "MANUAL":
                        manual_entries_count += 1

        if recent_1h_count > 0:
            manual_ratio = round((manual_entries_count / recent_1h_count) * 100, 1)

        stats = {
            "high_value_count": len(high_value),
            "refunds_last_10_count": len(refunds_last_10),
            "outside_hours_count": outside_hours_count,
            "manual_entry_ratio": manual_ratio
        }

        return stats

    except Exception as e:
        print(traceback.format_exc())
        return jsonify(content={"error": str(e)}, status_code=500)

def safe_datetime(ts: int):
    """Convert millisecond timestamp (int) → datetime (UTC)."""
    if ts is None:
        return None
    return datetime.utcfromtimestamp(int(ts) / 1000.0)

def clean_decimals(obj):
    """Recursively convert Decimal to int or float for JSON serialization."""
    if isinstance(obj, list):
        return [clean_decimals(x) for x in obj]
    if isinstance(obj, dict):
        return {k: clean_decimals(v) for k, v in obj.items()}
    if isinstance(obj, Decimal):
        return int(obj) if obj % 1 == 0 else float(obj)
    return obj

@app.route("/anomaly_transaction/list", methods=['GET'])
def get_paginated_anomaly_transactions():
    try:
        limit = int(request.args.get("limit", 10))
        sort_order = request.args.get("sort_order", "desc")  # "asc" or "desc"
        last_evaluated_key = request.args.get("last_evaluated_key")

        # Convert token back to dict
        lek = json.loads(last_evaluated_key) if last_evaluated_key else None

        # Fetch from DynamoDB
        trans_repo = AnomalyTransactionRepository(TABLE_ANOMALY_TRANSACTION)
        transactions, next_key = trans_repo.get_paginated_items(
            limit=limit,
            last_evaluated_key=lek,
            sort_order=sort_order
        )

        return jsonify({
            "items": [AnomalyTransaction.to_item(tx) for tx in transactions],
            "next_token": json.dumps(clean_decimals(next_key)) if next_key else None
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/batch_anomaly_transaction/list", methods=['GET'])
def get_paginated_batch_anomaly_transactions():
    try:
        limit = int(request.args.get("limit", 10))
        sort_order = request.args.get("sort_order", "desc")  # "asc" or "desc"
        last_evaluated_key = request.args.get("last_evaluated_key")

        # Convert token back to dict
        lek = json.loads(last_evaluated_key) if last_evaluated_key else None

        # Fetch from DynamoDB
        trans_repo = BatchAnomalyTransactionRepository(TABLE_BATCH_ANOMALY_TRANSACTION)
        transactions, next_key = trans_repo.get_paginated_items(
            limit=limit,
            last_evaluated_key=lek,
            sort_order=sort_order
        )

        return jsonify({
            "items": [BatchAnomalyTransaction.to_item(tx) for tx in transactions],
            "next_token": json.dumps(clean_decimals(next_key)) if next_key else None
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/batch_anomaly_transactions/metrics', methods=['GET'])
def get_batch_anomaly_metrics():
    try:
        repo = BatchAnomalyTransactionRepository()
        all_txns = repo.get_all_items()
        now = datetime.now(timezone.utc)

        def filter_and_count(since_dt: datetime):
            filtered = []
            for txn in all_txns:
                created_dt = to_dt_utc(getattr(txn, "timestamp_initiated", None))
                if created_dt and created_dt >= since_dt and getattr(txn, "is_anomaly", False):
                    filtered.append(txn)

            counter = Counter([(getattr(txn, "anomaly_type", None) or "unknown") for txn in filtered])
            return dict(counter)

        return jsonify({
            "last_24h": filter_and_count(now - timedelta(hours=24)),
            "last_week": filter_and_count(now - timedelta(days=7)),
            "last_month": filter_and_count(now - timedelta(days=30)),
        }), 200

    except Exception as e:
        print(traceback.format_exc())
        return jsonify({"error": str(e)}), 500

def bucketize(ts: int, bucket: str) -> int:
    """
    Floor a timestamp (epoch seconds) into the start of its bucket.
    Supported buckets: 5m, 15m, 1h, 1d
    """
    if isinstance(ts, datetime):
        ts = int(ts.timestamp())

    if bucket == "5m":
        return ts - (ts % (5 * 60))
    elif bucket == "15m":
        return ts - (ts % (15 * 60))
    elif bucket == "1h":
        return ts - (ts % (60 * 60))
    elif bucket == "1d":
        return ts - (ts % (24 * 60 * 60))
    else:
        raise ValueError(f"Unsupported bucket size: {bucket}")

if __name__ == '__main__':
    socketio.run(app, host="0.0.0.0", port=5000)
  #  app.run(debug=True)


