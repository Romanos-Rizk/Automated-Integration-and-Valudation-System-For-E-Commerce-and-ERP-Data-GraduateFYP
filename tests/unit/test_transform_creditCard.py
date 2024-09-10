'''
test_convert_data_types_Credit_Card:
    -Purpose: Verify that the convert_data_types_Credit_Card function correctly converts data types according to the specified dictionary.
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

from transform import convert_data_types_Credit_Card

def test_convert_data_types_Credit_Card():
    input_data = pd.DataFrame({
        'Value_date': ['2023-01-01', '2023-01-02'],
        'Narrative': ['Transaction 1', 'Transaction 2'],
        'Amount': [100.0, 200.0],
        'Currency': ['USD', 'EUR']
    })
    expected_output = input_data.astype({
        'Value_date': 'datetime64[ns]',
        'Narrative': 'str',
        'Amount': 'float32',
        'Currency': 'str'
    })
    result = convert_data_types_Credit_Card(input_data)
    assert_frame_equal(result, expected_output, check_dtype=True)
