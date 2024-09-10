import pytest
import pandas as pd
import os
import sys
script_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../scripts'))
sys.path.append(script_path)
from transform_pipelines import data_preprocessing_pipeline_ERP_Data

@pytest.fixture
def sample_erp_data():
    df = pd.DataFrame({
        'CUSTOMER_NUMBER': [123, 456, 789],
        'CUSTOMER_NAME': ['Acme Corp', ' Widget Works ', 'Gizmo Co'],
        'RECEIPT_CLASS': ['PREMIUM', 'Standard', ' basic '],
        'CURRENCY_CODE': ['usd', ' eur ', 'GBP'],
        'COMMENTS': ['Payment for Token A123', 'Refund Token B456', 'Charge for account']
    })
    return df

@pytest.fixture
def expected_output_erp_data():
    expected_df = pd.DataFrame({
        'CUSTOMER_NUMBER': ['123', '456', '789'],
        'CUSTOMER_NAME': ['Acme corp', 'Widget works', 'Gizmo co'],
        'RECEIPT_CLASS': ['Premium', 'Standard', 'Basic'],
        'CURRENCY_CODE': ['USD', 'EUR', 'GBP'],
        'COMMENTS': ['A123', 'B456', 'Charge for account']
    })
    expected_df['CUSTOMER_NUMBER'] = expected_df['CUSTOMER_NUMBER'].astype('str')
    return expected_df

def test_data_preprocessing_pipeline_erp_data(sample_erp_data, expected_output_erp_data):
    transformed_data = data_preprocessing_pipeline_ERP_Data(sample_erp_data)
    
    # Ensure CUSTOMER_NUMBER is string in both dataframes
    transformed_data['CUSTOMER_NUMBER'] = transformed_data['CUSTOMER_NUMBER'].astype('str')
    
    pd.testing.assert_frame_equal(transformed_data, expected_output_erp_data)