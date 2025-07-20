from flask import Flask, jsonify, url_for, send_from_directory
import os
from anomaly_detector import generate_and_process_data

app = Flask(__name__)
OUTPUT_FOLDER = "output"
OUTPUT_FILENAME = "transactions_with_anomalies.csv"

@app.route('/run-anomaly-detection', methods=['GET'])
def run_detection():
    output_path = generate_and_process_data()
    file_name = os.path.basename(output_path)
    download_link = url_for('download_file', filename=file_name, _external=True)
    return jsonify({
        "message": "Anomaly report generated successfully.",
        "download_link": download_link
    })

@app.route('/download/<filename>', methods=['GET'])
def download_file(filename):
    return send_from_directory(
        directory=os.path.abspath(OUTPUT_FOLDER),
        path=filename,
        as_attachment=True,
        mimetype='text/csv'
    )

if __name__ == '__main__':
    app.run(debug=True)
