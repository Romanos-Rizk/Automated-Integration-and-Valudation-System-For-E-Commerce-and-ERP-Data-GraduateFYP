import pytest
import pandas as pd
import os
import sys
script_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../scripts'))
sys.path.append(script_path)
from transform import concatenate_Aramex_Cosmaline

from transform_pipelines import (
    data_preprocessing_pipeline_ECOM_Data,
    data_preprocessing_pipeline_Aramex_Data,
    data_preprocessing_pipeline_Cosmaline_Data,
    data_preprocessing_pipeline_Cosmaline_Aramex_Data
)

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
def sample_aramex_data():
    return pd.DataFrame({
        'ShprNo': ['S1', 'S2', 'S3'],
        'HAWB': ['123-456', '234-567', '345-678'],
        'Delivery_Date': ['2023-01-02', '2023-01-03', '2023-01-04'],
        'CODAmount': [150.00, 250.00, 350.00],
        'O_CODAmount': [100.00, 200.00, 300.00],
        'Aramex_Inv_date': ['2023-01-02', '2023-01-03', '2023-01-04'],
        'TOKENNO': ['T1', 'T2', 'T3'],
        'CODCurrency': ['usd', 'eur', 'gbp']
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
def expected_output_concatenated_data():
    expected_df = pd.DataFrame({
        'ShprNo': ['S1', 'S2', 'S3', pd.NA, pd.NA, pd.NA],
        'AWB': ['123-456', '234-567', '345-678', '123-456', '234-567', '345-678'],
        'Delivery_Date': pd.to_datetime(['2023-01-02', '2023-01-03', '2023-01-04', '2023-01-02', '2023-01-03', '2023-01-04']),
        'CODAmount': pd.Series([150.00, 250.00, 350.00, 0, 100.00, 200.00], dtype='float32'),
        'O_CODAmount': pd.Series([100.00, 200.00, 300.00, 0, 100.00, 200.00], dtype='float32'),
        'Aramex_Inv_date': pd.Series([pd.Timestamp('2023-01-02'), pd.Timestamp('2023-01-03'), pd.Timestamp('2023-01-04'), pd.NaT, pd.NaT, pd.NaT], dtype='datetime64[ns]'),
        'TOKENNO': ['T1', 'T2', 'T3', 'Shipped With Cosmaline', 'Shipped With Cosmaline', 'Shipped With Cosmaline'],
        'CODCurrency': ['LBP', 'LBP', 'LBP', 'USD', 'LBP', 'LBP'],
        'order_number': ['123', '124', '125', '123', '124', '125']
    })
    return expected_df

def test_data_preprocessing_pipeline_cosmaline_aramex_data(sample_ecom_data, sample_aramex_data, sample_cosmaline_data, expected_output_concatenated_data):
    # First preprocess the ECOM data
    processed_ecom_data = data_preprocessing_pipeline_ECOM_Data(sample_ecom_data)
    
    # Preprocess the Aramex data
    processed_aramex_data = data_preprocessing_pipeline_Aramex_Data(sample_aramex_data, processed_ecom_data)
    
    # Preprocess the Cosmaline data
    processed_cosmaline_data = data_preprocessing_pipeline_Cosmaline_Data(sample_cosmaline_data, processed_ecom_data)
    
    # Concatenate the preprocessed Aramex and Cosmaline data
    concatenated_data = concatenate_Aramex_Cosmaline(processed_aramex_data, processed_cosmaline_data)
    
    # Ensure consistent null-like values for datetime columns
    concatenated_data['Aramex_Inv_date'] = pd.to_datetime(concatenated_data['Aramex_Inv_date'], errors='coerce')
    
    # Apply the final Cosmaline Aramex processing pipeline
    final_transformed_data = data_preprocessing_pipeline_Cosmaline_Aramex_Data(concatenated_data)
    
    # Ensure consistent null-like values for datetime columns
    final_transformed_data['Aramex_Inv_date'] = pd.to_datetime(final_transformed_data['Aramex_Inv_date'], errors='coerce')
    
    pd.testing.assert_frame_equal(final_transformed_data, expected_output_concatenated_data)