import pytest
import pandas as pd
import os
import sys
script_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../scripts'))
sys.path.append(script_path)
from transform_pipelines import data_preprocessing_pipeline_ECOM_Data, data_preprocessing_pipeline_Cosmaline_Data

@pytest.fixture
def sample_ecom_data():
    return pd.DataFrame({
        'Order Number': ['123', '124', '125'],
        'Shipper Name': ['Fedex', 'Ups', 'Dhl'],
        'Created At': ['2023-01-01 10:00:00', '2023-01-02 11:00:00', '2023-01-03 12:00:00'],
        'Delivered At': ['2023-01-02 10:00:00', '2023-01-03 11:00:00', '2023-01-04 12:00:00'],
        'Billing Type': ['Prepaid', 'Postpaid', 'Prepaid'],
        'Amount': [100.50, 200.75, 300.00],
        'Currency': ['USD', 'EUR', 'GBP'],
        'Country': ['US', 'FR', 'DE'],
        'AWB': ['123-456', '234-567', '345-678']
    })

@pytest.fixture
def sample_cosmaline_data():
    return pd.DataFrame({
        'Currency': ['usd', 'lbp', 'lbp'],
        'Driver_Delivery_date': ['2023-01-02', '2023-01-03', '2023-01-04'],
        'Amount': [0, 100.00, 200.00],
        'OrderNo': ['123', '124', '125']
    })

@pytest.fixture
def expected_output_cosmaline_data():
    expected_df = pd.DataFrame({
        'CODCurrency': ['USD', 'LBP', 'LBP'],
        'Delivery_Date': pd.to_datetime(['2023-01-02', '2023-01-03', '2023-01-04']),
        'O_CODAmount': pd.Series([0, 100.00, 200.00], dtype='float32'),
        'order_number': ['123', '124', '125'],
        'CODAmount': pd.Series([0, 100.00, 200.00], dtype='float32'),
        'ShprNo': pd.NA,
        'Aramex_Inv_date': pd.to_datetime([pd.NaT, pd.NaT, pd.NaT]),
        'TOKENNO': 'Shipped With Cosmaline',
        'AWB': ['123-456', '234-567', '345-678']
    })
    return expected_df

def test_data_preprocessing_pipeline_cosmaline_data(sample_cosmaline_data, sample_ecom_data, expected_output_cosmaline_data):
    # First preprocess the ECOM data
    processed_ecom_data = data_preprocessing_pipeline_ECOM_Data(sample_ecom_data)
    
    # Then preprocess the Cosmaline data
    transformed_cosmaline_data = data_preprocessing_pipeline_Cosmaline_Data(sample_cosmaline_data, processed_ecom_data)
    
    pd.testing.assert_frame_equal(transformed_cosmaline_data, expected_output_cosmaline_data)