'''
test_convert_data_types_ECOM_Data:
    -Purpose: Verify that the convert_data_types_ECOM_Data function correctly converts data types according to the specified dictionary.
    -Setup: Create an input DataFrame with sample data and an expected DataFrame with the correct types.
    -Assertion: Use assert_frame_equal to check if the function output matches the expected DataFrame.

test_rename_columns_for_website_ecom_data:
    -Purpose: Verify that the rename_columns_for_website_ecom_data function correctly renames columns based on a predefined mapping.
    -Setup: Create an input DataFrame and an expected DataFrame with renamed columns.
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

from transform import convert_data_types_ECOM_Data, rename_columns_for_website_ecom_data

def test_convert_data_types_ECOM_Data():
    input_data = pd.DataFrame({
        'Order Number': [123, 456],
        'Shipper Name': ['DHL', 'FedEx'],
        'Created At': ['2023-01-01', '2023-01-02'],
        'Delivered At': ['2023-01-03', '2023-01-04'],
        'Billing Type': ['Prepaid', 'COD'],
        'Amount': [100.0, 200.0],
        'Currency': ['USD', 'EUR'],
        'Country': ['US', 'DE'],
        'AWB': [789, 1011]
    })
    expected_output = input_data.astype({
        'Order Number': 'object',
        'Shipper Name': 'object',
        'Created At': 'datetime64[ns]',
        'Delivered At': 'datetime64[ns]',
        'Billing Type': 'object',
        'Amount': 'float64',
        'Currency': 'object',
        'Country': 'object',
        'AWB': 'object'
    })
    result = convert_data_types_ECOM_Data(input_data)
    assert_frame_equal(result, expected_output, check_dtype=True)

def test_rename_columns_for_website_ecom_data():
    input_data = pd.DataFrame({
        'Order Number': [123, 456],
        'Shipper Name': ['DHL', 'FedEx'],
        'Created At': ['2023-01-01', '2023-01-02'],
        'Delivered At': ['2023-01-03', '2023-01-04'],
        'Billing Type': ['Prepaid', 'COD'],
        'Amount': [100.0, 200.0],
        'Currency': ['USD', 'EUR'],
        'Country': ['US', 'DE'],
        'AWB': [789, 1011]
    })
    expected_output = input_data.rename(columns={
        'Order Number': 'order_number',
        'Shipper Name': 'shipper_name',
        'Created At': 'created_at',
        'Delivered At': 'delivered_at',
        'Billing Type': 'billing_type',
        'Amount': 'amount',
        'Currency': 'currency',
        'Country': 'country',
        'AWB': 'AWB'
    })
    result = rename_columns_for_website_ecom_data(input_data)
    assert_frame_equal(result, expected_output, check_dtype=True)

