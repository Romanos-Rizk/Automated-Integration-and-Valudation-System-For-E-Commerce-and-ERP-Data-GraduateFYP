'''
Mocks:
    -mock_send_email: Mocks the send_email function from Airflow to verify that it is called with the correct arguments without actually sending an email.

Test Functions:
    -test_format_failure_email_body: Verifies that the format_failure_email_body function correctly formats the failure email body.
    -test_format_success_email_body: Verifies that the format_success_email_body function correctly formats the success email body.
    -test_success_email: Verifies that the success_email function calls send_email with the correct arguments.
    -test_failure_email: Verifies that the failure_email function calls send_email with the correct arguments.

Assertions:
    -Ensure that the send_email function is called with the correct recipient, subject, and body.
'''

import pytest
from unittest.mock import patch, MagicMock
import sys
import os
script_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../scripts'))
sys.path.append(script_path)

import pytest
from unittest.mock import patch, MagicMock, mock_open
import importlib

# Mock environment variables to bypass Airflow initialization issues
@pytest.fixture(autouse=True)
def mock_airflow_env(monkeypatch):
    monkeypatch.setenv('AIRFLOW__CORE__SQL_ALCHEMY_CONN', 'sqlite:////tmp/airflow.db')
    monkeypatch.setenv('AIRFLOW__CORE__LOAD_EXAMPLES', 'False')
    monkeypatch.setenv('AIRFLOW__CORE__EXECUTOR', 'SequentialExecutor')

# Mocking the send_email function from Airflow
@pytest.fixture(autouse=True)
def mock_airflow_imports(monkeypatch):
    mock_send_email = MagicMock()
    monkeypatch.setattr('airflow.utils.email.send_email', mock_send_email)

    # Dynamically import the email_notifications module after mocking
    global email_notifications
    email_notifications = importlib.import_module('email_notifications')

def test_format_failure_email_body():
    task_instance_mock = MagicMock()
    task_instance_mock.task_id = 'dummy_task'
    task_instance_mock.dag_id = 'dummy_dag'
    task_instance_mock.execution_date = '2023-01-01'
    task_instance_mock.log_url = 'http://dummy_log_url'
    
    expected_body = f"""
    <html>
    <body>
        <h2>ETL Automated Process Notification</h2>
        <p><strong>Task:</strong> dummy_task</p>
        <p><strong>Status:</strong> Failed</p>
        <p><strong>DAG:</strong> dummy_dag</p>
        <p><strong>Execution Date:</strong> 2023-01-01</p>
        <p><strong>Log URL:</strong> <a href="http://dummy_log_url">http://dummy_log_url</a></p>
        <br>
        <p>Best Regards,</p>
        <p>Your ETL Automation System</p>
    </body>
    </html>
    """
    actual_body = email_notifications.format_failure_email_body(task_instance_mock, 'Failed')
    assert actual_body.strip() == expected_body.strip()

def test_format_success_email_body():
    expected_body = f"""
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
    actual_body = email_notifications.format_success_email_body()
    assert actual_body.strip() == expected_body.strip()

# Mocking the send_email function for success_email
@patch('email_notifications.send_email')
def test_success_email(mock_send_email):
    context = {}
    email_notifications.success_email(context)
    
    subject = 'ETL Process Success'
    body = email_notifications.format_success_email_body()
    to_email = ['rar37@mail.aub.edu', 'hmf31@mail.aub.edu']
    
    mock_send_email.assert_called_once_with(to=to_email, subject=subject, html_content=body)

# Mocking the send_email function for failure_email
@patch('email_notifications.send_email')
def test_failure_email(mock_send_email):
    task_instance_mock = MagicMock()
    task_instance_mock.task_id = 'dummy_task'
    task_instance_mock.dag_id = 'dummy_dag'
    task_instance_mock.execution_date = '2023-01-01'
    task_instance_mock.log_url = 'http://dummy_log_url'
    
    context = {'task_instance': task_instance_mock}
    email_notifications.failure_email(context)
    
    subject = 'Airflow Task dummy_task Failed'
    body = email_notifications.format_failure_email_body(task_instance_mock, 'Failed')
    to_email = ['rar37@mail.aub.edu', 'hmf31@mail.aub.edu']
    
    mock_send_email.assert_called_once_with(to=to_email, subject=subject, html_content=body)
