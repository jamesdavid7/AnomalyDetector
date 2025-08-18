from flask import Flask, jsonify, send_file, request

from api.utils import ses_utils
from services.anomaly_detector import generate_and_process_data
from config.constatns import S3_BUCKET_NAME, PROCESSED_DATA_DIR, TABLE_ANOMALY_METRICS, INPUT_DATA_DIR
from dynamodb.metric_data import MetricDataRepo
from models.metric import Metric
from utils.s3_utils import S3Utils
from services.csv_generation import save_transactions_to_csv
from services.anomaly_detector_read_s3 import process_csv_from_s3  # <-- adjust if needed
from services.CSVGenerator import generate_dataset
from threading import Thread
import joblib
import pandas as pd
from services.anomaly_rules import anomaly_rules, add_anomaly
from services.anomaly_detector_updated import process_csv_from_s3


app = Flask(__name__)
OUTPUT_FOLDER = "output"
OUTPUT_FILENAME = "transactions_with_anomalies.csv"
# Load models once (for performance)
rf_model = joblib.load("mlartifact/random_forest_model_all_params.pkl")
iso_model = joblib.load("mlartifact/isolation_forest_model_all_params.pkl")
encoder = joblib.load("mlartifact/categorical_encoder.pkl")
scaler = joblib.load("mlartifact/scaler.pkl")
training_stats = joblib.load("mlartifact/training_stats.pkl")  # dict with mean/std per column

feature_cols = [
    'amount', 'banking_charge', 'transaction_duration',
    'timestamp_initiated_epoch', 'timestamp_completed_epoch',
    'card_type_code', 'currency_code', 'terminal_currency_code'
]
numeric_cols = [
    'amount', 'banking_charge', 'transaction_duration',
    'timestamp_initiated_epoch', 'timestamp_completed_epoch'
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
def uploadFile():
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

    # Convert timestamps safely
    df['timestamp_initiated'] = pd.to_datetime(df['timestamp_initiated'], errors='coerce').fillna(pd.Timestamp.now())
    df['timestamp_completed'] = pd.to_datetime(df['timestamp_completed'], errors='coerce').fillna(pd.Timestamp.now())

    df['timestamp_initiated_epoch'] = df['timestamp_initiated'].apply(lambda x: int(x.timestamp()))
    df['timestamp_completed_epoch'] = df['timestamp_completed'].apply(lambda x: int(x.timestamp()))
    df['transaction_duration'] = (df['timestamp_completed_epoch'] - df['timestamp_initiated_epoch']) / 60

    # define feature columns here
    feature_cols = [
        'amount', 'banking_charge', 'transaction_duration',
        'timestamp_initiated_epoch', 'timestamp_completed_epoch',
        'card_type_code', 'currency_code', 'terminal_currency_code'
    ]
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

        return jsonify({
            "transaction_id": txn.get("transaction_id"),
            "customer_Name": txn.get("customer_Name"),
            "merchant_name": txn.get("merchant_name"),
            "store_name": txn.get("store_name"),
            "transaction_amount": txn.get("amount"),
            "is_anomaly": len(detections) > 0,
            "detections": detections
        }), 200

    except Exception as e:
        import traceback
        return jsonify({
            "error": str(e),
            "trace": traceback.format_exc()
        }), 500


if __name__ == '__main__':

  #  app.run(debug=True)

    app.run(host="0.0.0.0", port=5000, debug=True)
