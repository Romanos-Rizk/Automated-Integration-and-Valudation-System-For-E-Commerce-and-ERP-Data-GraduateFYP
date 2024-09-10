'''
test_remove_time_from_datetime_Oracle_Data:
    -Purpose: Verify that the remove_time_from_datetime_Oracle_Data function correctly removes the time portion from a datetime column while preserving the datatype.
    -Setup: Create an input DataFrame with datetime strings and an expected DataFrame with only dates.
    -Assertion: Use assert_frame_equal to check if the function output matches the expected DataFrame.

test_rename_columns_for_Oracle_data:
    -Purpose: Verify that the rename_columns_for_Oracle_data function correctly renames columns based on a predefined mapping.
    -Setup: Create an input DataFrame and an expected DataFrame with renamed columns.
    -Assertion: Use assert_frame_equal to check if the function output matches the expected DataFrame.

test_convert_data_types_Oracle_Data:
    -Purpose: Verify that the convert_data_types_Oracle_Data function correctly converts data types according to the specified dictionary.
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
    remove_time_from_datetime_Oracle_Data,
    rename_columns_for_Oracle_data,
    convert_data_types_Oracle_Data
)

def test_remove_time_from_datetime_Oracle_Data():
    input_data = pd.DataFrame({
        'date_column': ['2023-01-01 10:00:00', '2023-01-02 15:30:00']
    })
    expected_output = pd.DataFrame({
        'date_column': pd.to_datetime(['2023-01-01', '2023-01-02'])
    })
    result = remove_time_from_datetime_Oracle_Data(input_data, 'date_column')
    assert_frame_equal(result, expected_output, check_dtype=True)

def test_rename_columns_for_Oracle_data():
    input_data = pd.DataFrame({
        'OPERATING_UNIT_NAME': ['Unit1', 'Unit2'],
        'ORACLE_REFERENCE_ORDER_NUMBER': [123, 456],
        'ECOM_REFERENCE_ORDER_NUMBER': ['ORD001', 'ORD002'],
        'ORDER_TYPE': ['Type1', 'Type2'],
        'ORDER_TYPE_NAME': ['Type Name1', 'Type Name2'],
        'ORDERED_ITEM': ['Item1', 'Item2'],
        'ORDERED_QUANTITY': [1, 2],
        'PRICING_QUANTITY_UOM': ['UOM1', 'UOM2'],
        'UNIT_SELLING_PRICE (after discount without vat)': [100.0, 200.0],
        'UNIT_LIST_PRICE( original price before discount without vat)': [150.0, 250.0],
        'ORDERED_DATE': ['2023-01-01', '2023-01-02'],
        'CUSTOMER_NAME': ['Customer1', 'Customer2'],
        'TAX_CODE': ['TAX1', 'TAX2']
    })
    expected_output = input_data.rename(columns={
        'OPERATING_UNIT_NAME': 'operating_unit_name',
        'ORACLE_REFERENCE_ORDER_NUMBER': 'oracle_reference_order_number',
        'ECOM_REFERENCE_ORDER_NUMBER': 'ecom_reference_order_number',
        'ORDER_TYPE': 'order_type',
        'ORDER_TYPE_NAME': 'order_type_name',
        'ORDERED_ITEM': 'ordered_item',
        'ORDERED_QUANTITY': 'ordered_quantity',
        'PRICING_QUANTITY_UOM': 'pricing_quantity_uom',
        'UNIT_SELLING_PRICE (after discount without vat)': 'unit_selling_price',
        'UNIT_LIST_PRICE( original price before discount without vat)': 'unit_list_price',
        'ORDERED_DATE': 'ordered_date',
        'CUSTOMER_NAME': 'customer_name',
        'TAX_CODE': 'tax_code'
    })
    result = rename_columns_for_Oracle_data(input_data)
    assert_frame_equal(result, expected_output, check_dtype=True)

def test_convert_data_types_Oracle_Data():
    input_data = pd.DataFrame({
        'operating_unit_name': ['Unit1', 'Unit2'],
        'oracle_reference_order_number': [123, 456],
        'ecom_reference_order_number': ['ORD001', 'ORD002'],
        'order_type': ['Type1', 'Type2'],
        'order_type_name': ['Type Name1', 'Type Name2'],
        'ordered_item': ['Item1', 'Item2'],
        'ordered_quantity': [1, 2],
        'pricing_quantity_uom': ['UOM1', 'UOM2'],
        'unit_selling_price': [100.0, 200.0],
        'unit_list_price': [150.0, 250.0],
        'ordered_date': ['2023-01-01', '2023-01-02'],
        'customer_name': ['Customer1', 'Customer2'],
        'tax_code': ['TAX1', 'TAX2']
    })
    expected_output = input_data.astype({
        'operating_unit_name': 'object',
        'oracle_reference_order_number': 'int32',
        'ecom_reference_order_number': 'object',
        'order_type': 'object',
        'order_type_name': 'object',
        'ordered_item': 'object',
        'ordered_quantity': 'int32',
        'pricing_quantity_uom': 'object',
        'unit_selling_price': 'float32',
        'unit_list_price': 'float32',
        'ordered_date': 'datetime64[ns]',
        'customer_name': 'object',
        'tax_code': 'object'
    })
    expected_output['ordered_date'] = pd.to_datetime(expected_output['ordered_date'])
    result = convert_data_types_Oracle_Data(input_data)
    assert_frame_equal(result, expected_output, check_dtype=True)