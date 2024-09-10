'''
test_convert_data_types_ERP_Data:
    -Purpose: Verify that the convert_data_types_ERP_Data function correctly converts data types according to the specified dictionary.
    -Setup: Create an input DataFrame with sample data and an expected DataFrame with the correct types.
    -Assertion: Use assert_frame_equal to check if the function output matches the expected DataFrame.

test_extract_token_ERP_Data:
    -Purpose: Verify that the extract_token_ERP_Data function correctly extracts tokens from the specified column.
    -Setup: Create an input DataFrame with a column containing token strings and an expected DataFrame with extracted tokens.
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

from transform import convert_data_types_ERP_Data, extract_token_ERP_Data

def test_convert_data_types_ERP_Data():
    input_data = pd.DataFrame({
        'CUSTOMER_NUMBER': [12345, 67890],
        'OtherColumn': ['A', 'B']
    })
    expected_output = input_data.astype({
        'CUSTOMER_NUMBER': 'object'
    })
    result = convert_data_types_ERP_Data(input_data)
    assert_frame_equal(result, expected_output, check_dtype=True)

def test_extract_token_ERP_Data():
    input_data = pd.DataFrame({
        'description': ['Token 12345', 'No Number Here', 'Token 67890']
    })
    expected_output = pd.DataFrame({
        'description': ['12345', 'No Number Here', '67890']
    })
    result = extract_token_ERP_Data(input_data, 'description')
    assert_frame_equal(result, expected_output, check_dtype=True)

