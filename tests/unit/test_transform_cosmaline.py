'''
test_convert_data_types_Cosmaline_Data:
    -Purpose: Verify that convert_data_types_Cosmaline_Data function correctly converts data types according to the specified dictionary.
    -Setup: Create an input DataFrame with sample data and an expected DataFrame with the correct types.
    -Assertion: Use assert_frame_equal to check if the function output matches the expected DataFrame.

test_rename_columns_for_Cosmaline_data:
    -Purpose: Verify that the rename_columns_for_Cosmaline_data function correctly renames columns based on a predefined mapping.
    -Setup: Create an input DataFrame and an expected DataFrame with renamed columns.
    -Assertion: Use assert_frame_equal to check if the function output matches the expected DataFrame.

test_add_cod_amount_column_Cosmaline_data:
    -Purpose: Verify that add_cod_amount_column_Cosmaline_data function correctly adds the CODAmount column based on the CODCurrency column.
    -Setup: Create an input DataFrame with CODCurrency and O_CODAmount columns and an expected DataFrame with the new CODAmount column.
    -Assertion: Use assert_frame_equal to check if the function output matches the expected DataFrame.

test_add_new_columns_Cosmaline_Data:
    -Purpose: Verify that add_new_columns_Cosmaline_Data function correctly adds new columns ShprNo, Aramex_Inv_date, and TOKENNO with appropriate values.
    -Setup: Create an input DataFrame and an expected DataFrame with the new columns.
    -Assertion: Use assert_frame_equal to check if the function output matches the expected DataFrame.

test_add_AWB_Cosmaline_Data:
    -Purpose: Verify that add_AWB_Cosmaline_Data function correctly merges ecom_data into cosmaline_data on the order_number column.
    -Setup: Create input DataFrames for cosmaline_data and ecom_data, and an expected DataFrame with the merged result.
    -Assertion: Use assert_frame_equal to check if the function output matches the expected DataFrame.

test_convert_data_types_aramex_cosmaline:
    -Purpose: Verify that convert_data_types_aramex_cosmaline function correctly converts data types according to the specified dictionary.
    -Setup: Create an input DataFrame with sample data and an expected DataFrame with the correct types.
    -Assertion: Use assert_frame_equal to check if the function output matches the expected DataFrame.
'''

import pytest
import pandas as pd
import sys
import os
from pandas.testing import assert_frame_equal

# Add the scripts directory to the system path
script_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../scripts'))
sys.path.append(script_path)

from transform import (
    convert_data_types_Cosmaline_Data,
    rename_columns_for_Cosmaline_data,
    add_cod_amount_column_Cosmaline_data,
    add_new_columns_Cosmaline_Data,
    add_AWB_Cosmaline_Data,
    convert_data_types_aramex_cosmaline
)

def test_convert_data_types_Cosmaline_Data():
    input_data = pd.DataFrame({
        'Currency': ['USD', 'LBP'],
        'Driver_Delivery_date': ['2023-01-01', '2023-01-02'],
        'Amount': [100.0, 200.0],
        'OrderNo': [123, 456]
    })
    expected_output = input_data.astype({
        'Currency': 'object',
        'Driver_Delivery_date': 'datetime64[ns]',
        'Amount': 'float64',
        'OrderNo': 'object'
    })
    result = convert_data_types_Cosmaline_Data(input_data)
    assert_frame_equal(result, expected_output, check_dtype=True)

def test_rename_columns_for_Cosmaline_data():
    input_data = pd.DataFrame({
        'Driver_Delivery_date': ['2023-01-01', '2023-01-02'],
        'OrderNo': [123, 456],
        'Amount': [100.0, 200.0],
        'Currency': ['USD', 'LBP']
    })
    expected_output = input_data.rename(columns={
        'Driver_Delivery_date': 'Delivery_Date',
        'OrderNo': 'order_number',
        'Amount': 'O_CODAmount',
        'Currency': 'CODCurrency'
    })
    result = rename_columns_for_Cosmaline_data(input_data)
    assert_frame_equal(result, expected_output, check_dtype=True)

def test_add_cod_amount_column_Cosmaline_data():
    input_data = pd.DataFrame({
        'CODCurrency': ['USD', 'LBP'],
        'O_CODAmount': [100.0, 200.0]
    })
    expected_output = input_data.copy()
    expected_output['CODAmount'] = [0.0, 200.0]
    result = add_cod_amount_column_Cosmaline_data(input_data)
    assert_frame_equal(result, expected_output, check_dtype=True)

def test_add_new_columns_Cosmaline_Data():
    input_data = pd.DataFrame({
        'order_number': [123, 456]
    })
    expected_output = input_data.copy()
    expected_output['ShprNo'] = pd.NA
    expected_output['Aramex_Inv_date'] = pd.NA
    expected_output['TOKENNO'] = 'Shipped With Cosmaline'
    result = add_new_columns_Cosmaline_Data(input_data)
    assert_frame_equal(result, expected_output, check_dtype=True)

def test_add_AWB_Cosmaline_Data():
    cosmaline_data = pd.DataFrame({
        'order_number': [123, 456],
        'OtherColumn': ['A', 'B']
    })
    ecom_data = pd.DataFrame({
        'order_number': [123, 456],
        'AWB': [789, 1011]
    })
    expected_output = pd.DataFrame({
        'order_number': [123, 456],
        'OtherColumn': ['A', 'B'],
        'AWB': [789, 1011]
    })
    result = add_AWB_Cosmaline_Data(cosmaline_data, ecom_data)
    assert_frame_equal(result, expected_output, check_dtype=True)

def test_convert_data_types_aramex_cosmaline():
    input_data = pd.DataFrame({
        'AWB': [123, 456],
        'ShprNo': ['ABC', 'DEF'],
        'Delivery_Date': ['2023-01-01', '2023-01-02'],
        'CODAmount': [100.0, 200.0],
        'CODCurrency': ['USD', 'LBP'],
        'O_CODAmount': [150.0, 250.0],
        'Aramex_Inv_date': ['2023-01-03', '2023-01-04'],
        'TOKENNO': [5678, 91011],
        'order_number': [1122, 3344]
    })
    expected_output = input_data.astype({
        'AWB': 'str',
        'ShprNo': 'object',
        'Delivery_Date': 'datetime64[ns]',
        'CODAmount': 'float32',
        'CODCurrency': 'object',
        'O_CODAmount': 'float32',
        'Aramex_Inv_date': 'datetime64[ns]',
        'TOKENNO': 'object',
        'order_number': 'object'
    })
    result = convert_data_types_aramex_cosmaline(input_data)
    assert_frame_equal(result, expected_output, check_dtype=True)
