'''
Successful Data Read: We mock pd.read_excel to return a sample DataFrame. We then check if the function correctly calls pd.read_excel and returns the expected DataFrame.
Exception Handling: We mock pd.read_excel to raise an exception and check if the function correctly handles the exception and returns None.
'''

import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
import sys
import os
script_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../scripts'))
sys.path.append(script_path)
from extract import read_data_from_excel

# Test case for successful data read
def test_read_data_from_excel_success():
    # Sample data to be returned by the mock
    sample_data = pd.DataFrame({
        'column1': [1, 2],
        'column2': ['a', 'b']
    })
    
    # Patch the pd.read_excel method to return sample_data
    with patch('pandas.read_excel', return_value=sample_data) as mock_read_excel:
        filepath = 'dummy_path.xlsx'
        sheet_name = 'Sheet1'
        result = read_data_from_excel(filepath, sheet_name)
        
        # Assertions
        mock_read_excel.assert_called_once_with(filepath, sheet_name=sheet_name)
        assert result.equals(sample_data)

# Test case for exception handling
def test_read_data_from_excel_failure():
    # Patch the pd.read_excel method to raise an exception
    with patch('pandas.read_excel', side_effect=Exception("File not found")) as mock_read_excel:
        filepath = 'non_existent_file.xlsx'
        result = read_data_from_excel(filepath)
        
        # Assertions
        mock_read_excel.assert_called_once_with(filepath, sheet_name='Website ECOM Data')
        assert result is None
