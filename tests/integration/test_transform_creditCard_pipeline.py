import pytest
import pandas as pd
import os
import sys
script_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../scripts'))
sys.path.append(script_path)
from transform_pipelines import data_preprocessing_pipeline_CreditCard_Data

@pytest.fixture
def sample_credit_card_data():
    return pd.DataFrame({
        'Value_date': ['2023-01-01', '2023-01-02', '2023-01-03'],
        'Narrative': ['payment', ' refund', 'chargeback '],
        'Amount': ['100.50', '200.75', '300.00'],
        'Currency': ['usd', 'eur', ' gbp ']
    })

@pytest.fixture
def expected_output_credit_card_data():
    return pd.DataFrame({
        'value_date': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03']),
        'narrative': ['PAYMENT', 'REFUND', 'CHARGEBACK'],
        'amount': pd.Series([100.50, 200.75, 300.00], dtype='float32'),
        'currency': ['USD', 'EUR', 'GBP']
    })

def test_data_preprocessing_pipeline_credit_card_data(sample_credit_card_data, expected_output_credit_card_data):
    transformed_data = data_preprocessing_pipeline_CreditCard_Data(sample_credit_card_data)
    pd.testing.assert_frame_equal(transformed_data, expected_output_credit_card_data)