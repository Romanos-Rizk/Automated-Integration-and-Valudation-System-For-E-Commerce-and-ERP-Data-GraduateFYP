'''
Mocks:
    -mock_open: Mocks the file opening and reading operation to simulate reading an SQL script.
    -mock_create_db_engine: Mocks the create_db_engine function from load.py.
    -mock_sessionmaker: Mocks the SQLAlchemy sessionmaker.

Test Functions:
    -test_execute_reconciliation_script: Tests the normal execution path, verifying that the script is read, the SQL statements are executed, and the session is committed and closed.
    -test_execute_reconciliation_script_exception: Tests the exception handling path, verifying that the session is rolled back and closed if an exception occurs during execution.

Assertions:
    -Ensure that the create_db_engine and sessionmaker are called correctly.
    -Verify that the SQL script is read from the file.
    -Check that the SQL statements are executed.
    -Confirm that the session is committed or rolled back and closed as appropriate.
'''

import pytest
from unittest.mock import patch, MagicMock, mock_open
import sqlalchemy as sa
import sys
import os

# Add the scripts directory to the system path
script_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../scripts'))
sys.path.append(script_path)

from reconciliation import execute_reconciliation_script

# Mocking the create_db_engine function from load.py
@patch('reconciliation.create_db_engine')
@patch('reconciliation.sessionmaker')
@patch('builtins.open', new_callable=mock_open, read_data='SELECT * FROM test_table;')
def test_execute_reconciliation_script(mock_open, mock_sessionmaker, mock_create_db_engine):
    # Mock engine and session
    mock_engine = MagicMock()
    mock_create_db_engine.return_value = mock_engine

    mock_session = MagicMock()
    mock_sessionmaker.return_value = MagicMock(return_value=mock_session)

    # Call the function
    execute_reconciliation_script('dummy_script.sql')

    # Check that create_db_engine was called
    mock_create_db_engine.assert_called_once()

    # Check that sessionmaker was called with the mock engine
    mock_sessionmaker.assert_called_once_with(bind=mock_engine)

    # Check that the SQL script was read from the file
    mock_open.assert_called_once_with('/opt/airflow/scripts/dummy_script.sql', 'r')

    # Extract the executed SQL statement text
    executed_sql = mock_session.execute.call_args[0][0].text.strip()
    expected_sql = sa.text('SELECT * FROM test_table').text.strip()

    # Check that the SQL statements were executed
    assert executed_sql == expected_sql

    # Check that the session commit was called
    mock_session.commit.assert_called_once()

    # Check that the session was closed
    mock_session.close.assert_called_once()

# Test case for exception handling
@patch('reconciliation.create_db_engine')
@patch('reconciliation.sessionmaker')
@patch('builtins.open', new_callable=mock_open, read_data='SELECT * FROM test_table;')
def test_execute_reconciliation_script_exception(mock_open, mock_sessionmaker, mock_create_db_engine):
    # Mock engine and session
    mock_engine = MagicMock()
    mock_create_db_engine.return_value = mock_engine

    mock_session = MagicMock()
    mock_sessionmaker.return_value = MagicMock(return_value=mock_session)

    # Configure the session to raise an exception on execute
    mock_session.execute.side_effect = Exception('Execution error')

    # Call the function
    execute_reconciliation_script('dummy_script.sql')

    # Check that the session rollback was called
    mock_session.rollback.assert_called_once()

    # Check that the session was closed
    mock_session.close.assert_called_once()

if __name__ == '__main__':
    pytest.main()
