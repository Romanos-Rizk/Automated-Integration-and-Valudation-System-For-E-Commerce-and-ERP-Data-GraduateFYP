'''
test_convert_data_types_Aramex_Data:
    -Purpose: Verify that the function correctly converts data types.
    -Setup: Create an input DataFrame with sample data and an expected DataFrame with converted types.
    -Assertion: Check if the function output matches the expected DataFrame.

test_rename_columns_for_website_Aramex_data:
    -Purpose: Verify that the function correctly renames columns based on a predefined mapping.
    -Setup: Create an input DataFrame and an expected DataFrame with renamed columns.
    -Assertion: Check if the function output matches the expected DataFrame.

test_update_cod_currency_Aramex_data:
    -Purpose: Verify that the function updates the CODCurrency column based on the CODAmount column.
    -Setup: Create an input DataFrame and an expected DataFrame with updated CODCurrency values.
    -Assertion: Check if the function output matches the expected DataFrame.

test_clean_tokens_by_date_Aramex_Data:
    -Purpose: Verify that the function ensures each unique date has the same
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
    convert_data_types_Aramex_Data,
    rename_columns_for_website_Aramex_data,
    update_cod_currency_Aramex_data,
    clean_tokens_by_date_Aramex_Data,
    add_order_number_Aramex_Data,
    convert_data_types_aramex_cosmaline,
    concatenate_Aramex_Cosmaline,
)

from transform_pipelines import data_preprocessing_pipeline_Aramex_Data

def test_convert_data_types_Aramex_Data():
    input_data = pd.DataFrame({
        'ShprNo': [123, 456],
        'HAWB': [789, 1011],
        'Delivery_Date': ['2023-01-01', '2023-01-02'],
        'CODAmount': [100.0, 200.0],
        'O_CODAmount': [150.0, 250.0],
        'Aramex_Inv_date': ['2023-01-03', '2023-01-04'],
        'TOKENNO': [5678, 91011]
    })
    expected_output = input_data.astype({
        'ShprNo': 'object',
        'HAWB': 'object',
        'Delivery_Date': 'datetime64[ns]',
        'CODAmount': 'float64',
        'O_CODAmount': 'float64',
        'Aramex_Inv_date': 'datetime64[ns]',
        'TOKENNO': 'object'
    })
    result = convert_data_types_Aramex_Data(input_data)
    assert result.equals(expected_output)

def test_rename_columns_for_website_Aramex_data():
    input_data = pd.DataFrame({
        'HAWB': [123, 456],
        'Value': [789, 1011]
    })
    expected_output = input_data.rename(columns={'HAWB': 'AWB'})
    result = rename_columns_for_website_Aramex_data(input_data)
    assert result.equals(expected_output)

def test_update_cod_currency_Aramex_data():
    input_data = pd.DataFrame({
        'CODAmount': [100.0, 0.0],
        'CODCurrency': ['', '']
    })
    expected_output = pd.DataFrame({
        'CODAmount': [100.0, 0.0],
        'CODCurrency': ['LBP', '']
    })
    result = update_cod_currency_Aramex_data(input_data)
    assert result.equals(expected_output)

def test_clean_tokens_by_date_Aramex_Data():
    input_data = pd.DataFrame({
        'Aramex_Inv_date': ['2023-01-01', '2023-01-01', '2023-01-02'],
        'TOKENNO': [1111, 2222, 3333]
    })
    expected_output = pd.DataFrame({
        'Aramex_Inv_date': ['2023-01-01', '2023-01-01', '2023-01-02'],
        'TOKENNO': [1111, 1111, 3333]
    })
    result = clean_tokens_by_date_Aramex_Data(input_data, 'Aramex_Inv_date', 'TOKENNO')
    assert result.equals(expected_output)

def test_add_order_number_Aramex_Data():
    aramex_data = pd.DataFrame({
        'AWB': ['123', '456'],
        'OtherColumn': ['A', 'B']
    })
    ecom_data = pd.DataFrame({
        'AWB': ['123', '456'],
        'order_number': [789, 1011]
    })
    expected_output = pd.DataFrame({
        'AWB': ['123', '456'],
        'OtherColumn': ['A', 'B'],
        'order_number': [789, 1011]
    })
    result = add_order_number_Aramex_Data(aramex_data, ecom_data)
    assert result.equals(expected_output)

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
    assert result.equals(expected_output)

def test_concatenate_Aramex_Cosmaline():
    df1 = pd.DataFrame({
        'Column1': [1, 2],
        'Column2': ['A', 'B']
    })
    df2 = pd.DataFrame({
        'Column1': [3, 4],
        'Column2': ['C', 'D']
    })
    expected_output = pd.DataFrame({
        'Column1': [1, 2, 3, 4],
        'Column2': ['A', 'B', 'C', 'D']
    })
    result = concatenate_Aramex_Cosmaline(df1, df2)
    assert result.equals(expected_output)

