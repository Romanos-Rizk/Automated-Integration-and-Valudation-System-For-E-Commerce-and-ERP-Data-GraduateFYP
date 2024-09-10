import pytest
import pandas as pd
import os
import sys
script_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../scripts'))
sys.path.append(script_path)
from transform_pipelines import data_preprocessing_pipeline_ECOM_Data

@pytest.fixture
def sample_ecom_data():
    return pd.DataFrame({
        'Order Number': ['123', '124', '125'],
        'Shipper Name': [' FedEx ', ' UPS', 'DHL '],
        'Created At': ['2023-01-01 10:00:00', '2023-01-02 11:00:00', '2023-01-03 12:00:00'],
        'Delivered At': ['2023-01-02 10:00:00', '2023-01-03 11:00:00', '2023-01-04 12:00:00'],
        'Billing Type': [' Prepaid ', 'Postpaid ', ' prepaid '],
        'Amount': ['100.50', '200.75', '300.00'],
        'Currency': ['usd', 'eur', ' gbp '],
        'Country': ['us', 'fr', ' de '],
        'AWB': ['123-456', '234-567', '345-678']
    })

@pytest.fixture
def expected_output_ecom_data():
    return pd.DataFrame({
        'order_number': ['123', '124', '125'],
        'shipper_name': ['Fedex', 'Ups', 'Dhl'],
        'created_at': pd.to_datetime(['2023-01-01 10:00:00', '2023-01-02 11:00:00', '2023-01-03 12:00:00']),
        'delivered_at': pd.to_datetime(['2023-01-02 10:00:00', '2023-01-03 11:00:00', '2023-01-04 12:00:00']),
        'billing_type': ['Prepaid', 'Postpaid', 'Prepaid'],
        'amount': [100.50, 200.75, 300.00],
        'currency': ['USD', 'EUR', 'GBP'],
        'country': ['US', 'FR', 'DE'],
        'AWB': ['123-456', '234-567', '345-678']
    })

def test_data_preprocessing_pipeline_ecom_data(sample_ecom_data, expected_output_ecom_data):
    transformed_data = data_preprocessing_pipeline_ECOM_Data(sample_ecom_data)
    pd.testing.assert_frame_equal(transformed_data, expected_output_ecom_data)
