import pytest
import pandas as pd
import os
import sys

script_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../scripts'))
sys.path.append(script_path)
from transform_pipelines import data_preprocessing_pipeline_Oracle_Data

@pytest.fixture
def sample_oracle_data():
    df = pd.DataFrame({
        'OPERATING_UNIT_NAME': ['Unit1', 'Unit2', 'Unit3'],
        'ORACLE_REFERENCE_ORDER_NUMBER': [12345, 67890, 13579],
        'ECOM_REFERENCE_ORDER_NUMBER': [111, 222, 333],
        'ORDER_TYPE': ['Type1', 'Type2', 'Type3'],
        'ORDER_TYPE_NAME': ['Name1', 'Name2', 'Name3'],
        'ORDERED_ITEM': ['Item1', 'Item2', 'Item3'],
        'ORDERED_QUANTITY': [10, 20, 30],
        'PRICING_QUANTITY_UOM': ['UOM1', 'UOM2', 'UOM3'],
        'UNIT_SELLING_PRICE (after discount without vat)': [100.5, 200.75, 300.00],
        'UNIT_LIST_PRICE( original price before discount without vat)': [120.5, 220.75, 320.00],
        'ORDERED_DATE': ['2023-01-01 10:00:00', '2023-01-02 11:00:00', '2023-01-03 12:00:00'],
        'CUSTOMER_NAME': ['Customer1', 'Customer2', 'Customer3'],
        'TAX_CODE': ['TAX1', 'TAX2', 'TAX3']
    })
    df['ECOM_REFERENCE_ORDER_NUMBER'] = df['ECOM_REFERENCE_ORDER_NUMBER'].astype('str')
    return df

@pytest.fixture
def expected_output_oracle_data():
    expected_df = pd.DataFrame({
        'operating_unit_name': ['Unit1', 'Unit2', 'Unit3'],
        'oracle_reference_order_number': pd.Series([12345, 67890, 13579], dtype='int32'),
        'ecom_reference_order_number': pd.Series(['111', '222', '333'], dtype='object'),
        'order_type': ['Type1', 'Type2', 'Type3'],
        'order_type_name': ['Name1', 'Name2', 'Name3'],
        'ordered_item': ['Item1', 'Item2', 'Item3'],
        'ordered_quantity': pd.Series([10, 20, 30], dtype='int32'),
        'pricing_quantity_uom': ['UOM1', 'UOM2', 'UOM3'],
        'unit_selling_price': pd.Series([100.5, 200.75, 300.00], dtype='float32'),
        'unit_list_price': pd.Series([120.5, 220.75, 320.00], dtype='float32'),
        'ordered_date': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03']),
        'customer_name': ['Customer1', 'Customer2', 'Customer3'],
        'tax_code': ['TAX1', 'TAX2', 'TAX3']
    })
    return expected_df

def test_data_preprocessing_pipeline_oracle_data(sample_oracle_data, expected_output_oracle_data):
    transformed_data = data_preprocessing_pipeline_Oracle_Data(sample_oracle_data)
    
    # Ensure the data types match
    transformed_data['oracle_reference_order_number'] = transformed_data['oracle_reference_order_number'].astype('int32')
    transformed_data['ordered_quantity'] = transformed_data['ordered_quantity'].astype('int32')
    transformed_data['ecom_reference_order_number'] = transformed_data['ecom_reference_order_number'].astype('object')

    try:
        pd.testing.assert_frame_equal(transformed_data, expected_output_oracle_data)
    except AssertionError as e:
        print("AssertionError:", e)
        print("Transformed DataFrame:\n", transformed_data)
        print("Transformed DataFrame Types:\n", transformed_data.dtypes)
        print("Expected DataFrame:\n", expected_output_oracle_data)
        print("Expected DataFrame Types:\n", expected_output_oracle_data.dtypes)
        raise