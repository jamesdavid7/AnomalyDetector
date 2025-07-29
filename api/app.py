from flask import Flask, jsonify, send_file, request

from services.anomaly_detector import generate_and_process_data
from config.constatns import S3_BUCKET_NAME, PROCESSED_DATA_DIR, TABLE_ANOMALY_METRICS, INPUT_DATA_DIR
from dynamodb.metric_data import MetricDataRepo
from models.metric import Metric
from utils.s3_utils import S3Utils
from services.csv_generation import save_transactions_to_csv
from services.anomaly_detector_read_s3 import process_csv_from_s3  # <-- adjust if needed


app = Flask(__name__)

OUTPUT_FOLDER = "output"
OUTPUT_FILENAME = "transactions_with_anomalies.csv"

@app.route('/run-anomaly-detection', methods=['GET'])
def run_detection():
    output_path = generate_and_process_data()
    s3_utils = S3Utils(bucket_name=S3_BUCKET_NAME)
    filename = s3_utils.send_file_to_s3(output_path, PROCESSED_DATA_DIR)
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

if __name__ == '__main__':
    '''
    app.run(debug=True)
    '''
    app.run(host="0.0.0.0", port=5000, debug=True)
