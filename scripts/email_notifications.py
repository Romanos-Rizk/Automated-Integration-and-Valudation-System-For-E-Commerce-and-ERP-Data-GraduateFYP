# email_notifications.py
from airflow.utils.email import send_email
from airflow.models import TaskInstance
from airflow.utils.log.file_task_handler import FileTaskHandler
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email import encoders
import os

def format_failure_email_body(task_instance, task_status):
    return f"""
    <html>
    <body>
        <h2>ETL Automated Process Notification</h2>
        <p><strong>Task:</strong> {task_instance.task_id}</p>
        <p><strong>Status:</strong> {task_status}</p>
        <p><strong>DAG:</strong> {task_instance.dag_id}</p>
        <p><strong>Execution Date:</strong> {task_instance.execution_date}</p>
        <p><strong>Log URL:</strong> <a href="{task_instance.log_url}">{task_instance.log_url}</a></p>
        <br>
        <p>Best Regards,</p>
        <p>Your ETL Automation System</p>
    </body>
    </html>
    """
def format_success_email_body():
    return f"""
    <html>
    <body>
        <h2>ETL Automated Process Notification</h2>
        <p>The ETL process has completed successfully.</p>
        <p>All tasks have been executed without any issues.</p>
        <br>
        <p>Best Regards,</p>
        <p>Your ETL Automation System</p>
    </body>
    </html>
    """
    
def success_email(context):
    subject = 'ETL Process Success'
    body = format_success_email_body()
    to_email = ['rar37@mail.aub.edu', 'hmf31@mail.aub.edu']
    send_email(to=to_email, subject=subject, html_content=body)
    
def failure_email(context):
    task_instance = context['task_instance']
    task_status = 'Failed'
    subject = f'Airflow Task {task_instance.task_id} {task_status}'
    body = format_failure_email_body(task_instance, task_status)
    to_email = ['rar37@mail.aub.edu', 'hmf31@mail.aub.edu']
    send_email(to=to_email, subject=subject, html_content=body)
    
def send_sales_report_email(report_path):
    subject = 'Sales Report'
    body = """
    <html>
    <body>
        <h2>Sales Report</h2>
        <p>Please find attached the latest sales report.</p>
        <br>
        <p>Best Regards,</p>
        <p>Your ETL Automation System</p>
    </body>
    </html>
    """
    to_email = ['rar37@mail.aub.edu', 'hmf31@mail.aub.edu']
    attachments = [report_path]

    msg = MIMEMultipart()
    msg['Subject'] = subject
    msg['From'] = 'youremail@example.com'
    msg['To'] = ', '.join(to_email)
    msg.attach(MIMEText(body, 'html'))

    # Attach the sales report PDF
    for attachment in attachments:
        part = MIMEBase('application', "octet-stream")
        with open(attachment, 'rb') as file:
            part.set_payload(file.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment', filename=os.path.basename(attachment))
        msg.attach(part)

    send_email(to=to_email, subject=subject, html_content=body, files=[report_path])