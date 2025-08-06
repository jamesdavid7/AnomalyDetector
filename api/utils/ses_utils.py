import os
import shutil
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import pandas as pd
import win32com.client as win32

from api.config.aws_config import AWSConfig
from api.config.constatns import PROCESSED_DATA_DIR

SENDER = "jamesdavidbe@gmail.com"
RECIPIENTS = ["jamesdavidbe@gmail.com", "smtdhinesh@gmail.com"]
PASSWORD = "Admin@123"
EMAIL_SUBJECT = "Anomaly Detection Report"
EMAIL_BODY = f"""Hello,

Please find the password-protected anomaly report attached.

Regards,
Automated System
"""
ses = AWSConfig.get_ses_client()

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def send_email_with_attachment(sender, recipients, subject, body, attachment_path, attachment_name):
    msg = MIMEMultipart()
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)

    msg.attach(MIMEText(body, "plain"))

    with open(attachment_path, "rb") as f:
        part = MIMEApplication(f.read())
        part.add_header("Content-Disposition", f"attachment; filename={attachment_name}")
        msg.attach(part)

    # Send the email via SES
    response = ses.send_raw_email(
        Source=sender,
        Destinations=recipients,
        RawMessage={"Data": msg.as_string()}
    )
    print("✅ Email sent! to "+str(recipients)+" Message ID:", response["MessageId"])

def process_and_send_file(csv_path):

    excel_path = os.path.join(BASE_DIR, PROCESSED_DATA_DIR, "transactions_with_anomalies" + ".xlsx")
    print(excel_path)

    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found at: {csv_path}")


    # Step 1: Convert CSV to excel
    convert_csv_to_excel(csv_path, excel_path)

    # Step 2: Set password on the excel(.xslx) file
    set_password_on_excel(excel_path, PASSWORD)

    send_email_with_attachment(
        sender=SENDER,
        recipients=RECIPIENTS,
        subject=EMAIL_SUBJECT,
        body=EMAIL_BODY,
        attachment_path=excel_path,
        attachment_name=os.path.basename(excel_path)
    )

def convert_csv_to_excel(csv_path, excel_path):
    """ Convert the CSV file to Excel """
    print(f"Converting CSV to Excel: {csv_path} -> {excel_path}")
    df = pd.read_csv(csv_path)
    df.to_excel(excel_path, index=False)
    print(f"✅ Conversion successful! Excel saved at {excel_path}")

def set_password_on_excel(excel_path, password):
    """ Set open password on the Excel file (Windows only, Flask/thread-safe) """
    import pythoncom
    pythoncom.CoInitialize()  # ✅ Ensure COM is initialized in this thread

    try:
        """ Set password on the Excel file (Windows only) """
        print(f"Setting password on Excel file: {excel_path}")
        excel = win32.Dispatch('Excel.Application')
        excel.Visible = False  # Don't show Excel window
        wb = excel.Workbooks.Open(excel_path)
        temp_path = excel_path.replace(".xlsx", "_protected.xlsx")
        wb.SaveAs(temp_path, FileFormat=51, Password=password)
        wb.Close()
        excel.Quit()
        # Replace original with password-protected file
        os.remove(excel_path)
        shutil.move(temp_path, excel_path)
        print(f"✅ Password set and file replaced: {excel_path}")
    finally:
        pythoncom.CoUninitialize()  # ✅ Always uninitialize COM

# === EXECUTE ===
if __name__ == "__main__":
    path = os.path.join(BASE_DIR, PROCESSED_DATA_DIR, "transactions_with_anomalies.csv")
    print(path)
    process_and_send_file(path)