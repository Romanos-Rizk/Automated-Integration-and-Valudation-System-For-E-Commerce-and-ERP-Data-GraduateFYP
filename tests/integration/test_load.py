'''
test_create_db_engine:
    -Purpose: Verify that create_db_engine function correctly creates an SQLAlchemy engine.
    -Mocking: Use patch to mock create_engine.
    -Assertion: Check if the engine is created with the correct parameters.

test_upload_dataframe_to_temp_sql:
    -Purpose: Verify that upload_dataframe_to_temp_sql function correctly uploads a DataFrame to a temporary SQL table.
    -Mocking: Use patch to mock create_engine and SQLAlchemy connection.
    -Assertion: Check if the DataFrame's to_sql method is called with the correct parameters.

test_merge_data_from_temp_to_main_with_pk:
    -Purpose: Verify that merge_data_from_temp_to_main_with_pk function correctly merges data from a temporary table to the main table with a primary key.
    -Mocking: Use patch to mock create_engine and inspect.
    -Assertion: Check if the SQL execute method is called with the correct merge SQL statement.

test_merge_data_from_temp_to_main_without_pk:
    -Purpose: Verify that merge_data_from_temp_to_main_without_pk function correctly merges data from a temporary table to the main table without a primary key.
    -Mocking: Use patch to mock create_engine.
    -Assertion: Check if the SQL execute method is called with the correct merge SQL statement.

test_cleanup_temp_tables:
    -Purpose: Verify that cleanup_temp_tables function correctly drops temporary tables.
    -Mocking: Use patch to mock create_engine.
    -Assertion: Check if the SQL execute method is called with the correct drop table SQL statements.
'''

import pytest
import pandas as pd
import sys
import os
from unittest.mock import patch, MagicMock, call
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import inspect

# Add the scripts directory to the system path
script_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../scripts'))
sys.path.append(script_path)

from load import create_db_engine, upload_dataframe_to_temp_sql, merge_data_from_temp_to_main_with_pk, merge_data_from_temp_to_main_without_pk, cleanup_temp_tables

# Mock for create_engine to avoid actual database connections
@patch('load.create_engine')
def test_create_db_engine(mock_create_engine):
    mock_engine = MagicMock()
    mock_create_engine.return_value = mock_engine
    engine = create_db_engine()
    assert engine == mock_engine
    mock_create_engine.assert_called_once_with('mysql+pymysql://root:2452002Az)@host.docker.internal/CapstoneTest', echo=True)

# Mocking the SQLAlchemy engine and connection for upload_dataframe_to_temp_sql
@patch('load.create_engine')
def test_upload_dataframe_to_temp_sql(mock_create_engine):
    mock_engine = MagicMock()
    mock_create_engine.return_value = mock_engine
    mock_connection = mock_engine.connect.return_value.__enter__.return_value
    
    df = pd.DataFrame({'column1': [1, 2], 'column2': ['a', 'b']})
    with patch.object(df, 'to_sql') as mock_to_sql:
        upload_dataframe_to_temp_sql(df, 'temp_table', mock_engine)
        mock_to_sql.assert_called_once_with(name='temp_table', con=mock_engine, if_exists='replace', index=False)

# Mocking the SQLAlchemy engine and connection for merge_data_from_temp_to_main_with_pk
@patch('load.create_engine')
@patch('load.inspect')
def test_merge_data_from_temp_to_main_with_pk(mock_inspect, mock_create_engine):
    mock_engine = MagicMock()
    mock_create_engine.return_value = mock_engine
    mock_inspect.return_value.get_columns.return_value = [{'name': 'col1'}, {'name': 'col2'}, {'name': 'id'}]
    mock_connection = mock_engine.connect.return_value.__enter__.return_value
    
    merge_data_from_temp_to_main_with_pk('temp_table', 'main_table', mock_engine)
    
    mock_inspect.assert_called_once_with(mock_engine)
    mock_engine.connect.assert_called_once()
    expected_sql = (
        "INSERT INTO main_table (col1, col2)\n"
        "    SELECT col1, col2 FROM temp_table\n"
        "    ON DUPLICATE KEY UPDATE\n"
        "    col1 = VALUES(col1), col2 = VALUES(col2)"
    )
    mock_connection.execute.assert_called_once()
    actual_sql = mock_connection.execute.call_args[0][0].strip()
    print(f"Expected SQL:\n{expected_sql}")
    print(f"Actual SQL:\n{actual_sql}")
    assert actual_sql == expected_sql.strip()

@patch('load.create_engine')
def test_merge_data_from_temp_to_main_without_pk(mock_create_engine):
    mock_engine = MagicMock()
    mock_create_engine.return_value = mock_engine
    mock_connection = mock_engine.connect.return_value.__enter__.return_value

    merge_data_from_temp_to_main_without_pk('temp_table', 'main_table', mock_engine, ['col1', 'col2'])

    mock_engine.connect.assert_called_once()
    expected_sql = (
        "INSERT INTO main_table (col1, col2)\n"
        "    SELECT temp_table.col1, temp_table.col2\n"
        "    FROM temp_table\n"
        "    WHERE NOT EXISTS (\n"
        "        SELECT 1\n"
        "        FROM main_table\n"
        "        WHERE main_table.col1 = temp_table.col1 AND main_table.col2 = temp_table.col2\n"
        "    )"
    )
    mock_connection.execute.assert_called_once()
    actual_sql = mock_connection.execute.call_args[0][0].strip()

    def normalize_whitespace(sql):
        return ' '.join(sql.split())

    normalized_expected_sql = normalize_whitespace(expected_sql)
    normalized_actual_sql = normalize_whitespace(actual_sql)

    print(f"Expected SQL:\n{normalized_expected_sql}")
    print(f"Actual SQL:\n{normalized_actual_sql}")

    assert normalized_actual_sql == normalized_expected_sql

# Mocking the SQLAlchemy engine and connection for cleanup_temp_tables
@patch('load.create_engine')
def test_cleanup_temp_tables(mock_create_engine):
    mock_engine = MagicMock()
    mock_create_engine.return_value = mock_engine
    mock_connection = mock_engine.connect.return_value.__enter__.return_value
    
    cleanup_temp_tables(mock_engine)
    
    mock_engine.connect.assert_called_once()
    expected_calls = [
        call(f"DROP TABLE IF EXISTS {table}") for table in [
            'temp_ecom_orders', 
            'temp_aramex_data', 
            'temp_cosmaline_data', 
            'temp_oracle_data', 
            'temp_credit_card_data', 
            'temp_erp_data', 
            'temp_daily_rate',
            'temp_credit_card',
            'temp_ecom_orders',
            'temp_shippedandcollected_aramex_cosmaline',
            'erp_data_tokens',
            'aramex_cod_sum',
            'erp_receipt_sum',
            'erp_cosmaline',
            'aramex_cosmaline_sum',
            'erp_cosmaline_sum',
            'erp_cybersource_sum',
            'credit_card_sum' 
        ]
    ]
    mock_connection.execute.assert_has_calls(expected_calls, any_order=True)
