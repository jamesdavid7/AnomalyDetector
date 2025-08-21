from boto3.dynamodb import table
from botocore.exceptions import ClientError
from flask import Flask, jsonify, send_file, request

from api.dynamodb.transaction_data import TransactionRepo
from api.utils import ses_utils
from services.anomaly_detector import generate_and_process_data
from config.constatns import S3_BUCKET_NAME, PROCESSED_DATA_DIR, TABLE_ANOMALY_METRICS, INPUT_DATA_DIR, \
    TABLE_TRANSACTION
from dynamodb.metric_data import MetricDataRepo
from models.metric import Metric
from utils.s3_utils import S3Utils
from services.csv_generation import save_transactions_to_csv
from services.anomaly_detector_read_s3 import process_csv_from_s3  # <-- adjust if needed
from threading import Thread


app = Flask(__name__)

OUTPUT_FOLDER = "output"
OUTPUT_FILENAME = "transactions_with_anomalies.csv"

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

@app.route("/transactions", methods=["GET"])
def get_transactions():
    """
    Retrieve transactions from DynamoDB with pagination.
    Query Params:
        limit: int (default=10)
        last_evaluated_key: str (optional, JSON string from previous response)
    """
    try:
        # Get query params
        limit = int(request.args.get("limit", 10))
        last_evaluated_key = request.args.get("last_evaluated_key")

        import json
        if last_evaluated_key:
            try:
                last_evaluated_key = json.loads(last_evaluated_key)
            except json.JSONDecodeError:
                return jsonify({"error": "Invalid last_evaluated_key format"}), 400
        else:
            last_evaluated_key = None

        # Use repo
        repo = TransactionRepo(TABLE_TRANSACTION)
        result = repo.get_transactions_paginated(limit=limit, last_evaluated_key=last_evaluated_key)

        return jsonify(result), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    '''
    app.run(debug=True)
    '''
    app.run(host="0.0.0.0", port=5000, debug=True)
