'''
test_lower_columns:
    -Purpose: Verify that the lower_columns function correctly strips whitespace, converts text to lowercase, and capitalizes the first letter of each word in the specified columns.
    -Setup: Create a DataFrame with varying cases and whitespace.
    -Assertion: Check if the function output matches the expected DataFrame.

test_uppercase_columns:
    -Purpose: Verify that the uppercase_columns function correctly strips whitespace and converts text to uppercase in the specified columns.
    -Setup: Create a DataFrame with varying cases and whitespace.
    -Assertion: Check if the function output matches the expected DataFrame.

test_remove_missing_values:
    -Purpose: Verify that the remove_missing_values function correctly removes rows with any missing values.
    -Setup: Create a DataFrame with some missing values.
    -Assertion: Check if the function output matches the expected DataFrame with rows containing missing values removed.

test_remove_duplicates:
    -Purpose: Verify that the remove_duplicates function correctly removes duplicate rows, keeping the first occurrence.
    -Setup: Create a DataFrame with duplicate rows.
    -Assertion: Check if the function output matches the expected DataFrame with duplicates removed.

test_rename_columns_to_lowercase:
    -Purpose: Verify that the rename_columns_to_lowercase function correctly renames columns to lowercase.
    -Setup: Create a DataFrame with uppercase column names.
    -Assertion: Check if the function output matches the expected DataFrame with lowercase column names.
'''

import pytest
import pandas as pd
import sys
import os

# Add the scripts directory to the system path
script_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../scripts'))
sys.path.append(script_path)

from transform import lower_columns, uppercase_columns, remove_missing_values, remove_duplicates, rename_columns_to_lowercase

def test_lower_columns():
    input_data = pd.DataFrame({
        'Name': [' Romanos ', 'HADIL', ' ali '],
        'Age': [25, 30, 35]
    })
    columns = ['Name']
    expected_output = pd.DataFrame({
        'Name': ['Romanos', 'Hadil', 'Ali'],
        'Age': [25, 30, 35]
    })
    result = lower_columns(input_data, columns)
    assert result.equals(expected_output)

def test_uppercase_columns():
    input_data = pd.DataFrame({
        'Name': [' Romanos ', 'hadil', ' ali '],
        'Age': [25, 30, 35]
    })
    columns = ['Name']
    expected_output = pd.DataFrame({
        'Name': ['ROMANOS', 'HADIL', 'ALI'],
        'Age': [25, 30, 35]
    })
    result = uppercase_columns(input_data, columns)
    assert result.equals(expected_output)

def test_remove_missing_values():
    input_data = pd.DataFrame({
        'Name': ['Romanos', None, 'Ali'],
        'Age': [25, 30, None]
    })
    expected_output = pd.DataFrame({
        'Name': ['Romanos'],
        'Age': [25]
    })
    expected_output['Age'] = expected_output['Age'].astype(float)  # Ensure matching data types
    result = remove_missing_values(input_data)
    assert result.equals(expected_output)

def test_remove_duplicates():
    input_data = pd.DataFrame({
        'Name': ['Romanos', 'Hadil', 'Romanos'],
        'Age': [25, 30, 25]
    })
    expected_output = pd.DataFrame({
        'Name': ['Romanos', 'Hadil'],
        'Age': [25, 30]
    })
    result = remove_duplicates(input_data)
    assert result.equals(expected_output)

def test_rename_columns_to_lowercase():
    input_data = pd.DataFrame({
        'NAME': ['Romanos', 'Hadil', 'Ali'],
        'AGE': [25, 30, 35]
    })
    expected_output = pd.DataFrame({
        'name': ['Romanos', 'Hadil', 'Ali'],
        'age': [25, 30, 35]
    })
    result = rename_columns_to_lowercase(input_data)
    assert result.equals(expected_output)