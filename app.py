import joblib
import pandas as pd
from config.constatns import S3_BUCKET_NAME, PROCESSED_DATA_DIR, TABLE_ANOMALY_METRICS, INPUT_DATA_DIR
from dynamodb.metric import Metric
from dynamodb.metric_data import MetricDataRepo
from flask import Flask, jsonify, send_file, request
from services.anomaly_detector_read_s3 import process_csv_from_s3
from services.csv_generation import save_transactions_to_csv
from utils.s3_utils import S3Utils

# Load models once (for performance)
rf_model = joblib.load("models/random_forest_model.pkl")
iso_model = joblib.load("models/isolation_forest_model.pkl")


app = Flask(__name__)

OUTPUT_FOLDER = "output"
OUTPUT_FILENAME = "transactions_with_anomalies.csv"


def prepare_features(data):
    df = pd.DataFrame([data])
    df['timestamp_initiated'] = pd.to_datetime(df['timestamp_initiated'])
    df['timestamp_completed'] = pd.to_datetime(df['timestamp_completed'])

    # Calculate transaction_duration (minutes)
    df['transaction_duration'] = (df['timestamp_completed'] - df['timestamp_initiated']).dt.total_seconds() / 60
    df['currency_mismatch_flag'] = (df['currency'] != df['terminal_currency']).astype(int)
    # banking_charge fallback: from input or 1% of amount
    if 'banking_charge' in data and data['banking_charge'] is not None:
        df['banking_charge'] = float(data['banking_charge'])
    else:
        df['banking_charge'] = round(df['amount'].iloc[0] * 0.01, 2)

    df['charge_percent'] = df['banking_charge'] / df['amount']
    df['amount_per_minute'] = df['amount'] / (df['transaction_duration'] + 0.1)

    feature_cols = [
        'amount', 'banking_charge',
        'transaction_duration',
        'currency_mismatch_flag',  'amount_per_minute'
    ]

    # Reindex ensures all required columns are present, missing columns filled with 0
    features = df.reindex(columns=feature_cols, fill_value=0)

    return features


@app.route('/files/upload', methods=['GET'])
def uploadFile():
    output_path = save_transactions_to_csv()
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


@app.route("/files/processAnomaly", methods=["POST"])
def process_anomaly():
    try:
        data = request.get_json(force=True)
        bucket = data["bucket"]
        key = data["key"]
        result_key = process_csv_from_s3(bucket, key)
        return jsonify({"status": "success", "processed_key": result_key}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/transactions/detect-anomaly", methods=["POST"])
def detect_single_anomaly():
    try:
        data = request.get_json(force=True)
        features = prepare_features(data)

        rf_pred = rf_model.predict(features)[0] == 1
        iso_pred = iso_model.predict(features)[0] == -1
        iso_score= iso_model.decision_function(features).reshape(-1, 1)

        return jsonify({
            "transaction_id": data.get("transaction_id", "N/A"),
            "supervised_anomaly_detected": bool(rf_pred),
            "unsupervised_anomaly_detected": bool(iso_pred),
            "unsupervised_anomaly_score": float(iso_score)
        }), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True)
