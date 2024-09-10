import pandas as pd
import numpy as np
import ast
import re

def lower_columns(dataframe, columns):
    """
    Strips, lowers, and capitalizes the first letter of each word in the specified columns of a DataFrame.

    Parameters:
        dataframe (pd.DataFrame): The DataFrame to process.
        columns (list): List of column names to normalize.

    Returns:
        pd.DataFrame: The DataFrame with normalized columns.
    """
    for column in columns:
        if column in dataframe.columns:
            dataframe[column] = dataframe[column].str.strip().str.lower().str.capitalize()
        else:
            print(f"Warning: {column} does not exist in the DataFrame.")
    return dataframe

def uppercase_columns(dataframe, columns):
    """
    Strips whitespace and converts the text to upper case for specified columns in a DataFrame.

    Parameters:
        dataframe (pd.DataFrame): The DataFrame to process.
        columns (list): List of column names to convert to upper case.

    Returns:
        pd.DataFrame: The DataFrame with the specified columns converted to upper case.
    """
    for column in columns:
        if column in dataframe.columns:
            dataframe[column] = dataframe[column].str.strip().str.upper()
        else:
            print(f"Warning: {column} does not exist in the DataFrame.")
    return dataframe


def remove_missing_values(df):
    """
    Removes rows with any missing values from the DataFrame without printing any output.
    
    Parameters:
        df (pd.DataFrame): The DataFrame from which to remove missing values.
        
    Returns:
        pd.DataFrame: A new DataFrame with missing values removed.
    """
    # Remove rows with any missing values and return the new DataFrame
    return df.dropna()

def remove_duplicates(df):
    """
    Removes duplicate rows from the DataFrame, keeping the first occurrence of each duplicate.

    Parameters:
        df (pd.DataFrame): The DataFrame from which to remove duplicates.
        
    Returns:
        pd.DataFrame: A new DataFrame with duplicates removed, keeping the first occurrence.
    """
    return df.drop_duplicates()

def convert_data_types_ECOM_Data(df):
    """
    Converts the data types of specified columns in the DataFrame.
    
    Parameters:
        df (pd.DataFrame): The DataFrame whose columns will have their data types converted.
        
    Returns:
        pd.DataFrame: The DataFrame with converted data types.
    """
    # Define a dictionary mapping column names to desired data types
    dtype_dict = {
        'Order Number': 'object',
        'Shipper Name': 'object',
        'Created At': 'datetime64[ns]',
        'Delivered At': 'datetime64[ns]',
        'Billing Type': 'object',
        'Amount': 'float64',
        'Currency': 'object',
        'Country': 'object',
        'AWB': 'object'
    }
    
    # Convert data types according to the dictionary
    for column, dtype in dtype_dict.items():
        if column in df.columns:
            try:
                df[column] = df[column].astype(dtype)
            except ValueError:
                print(f"Conversion error: Could not convert {column} to {dtype}")
        else:
            print(f"Warning: {column} not in DataFrame")
    
    return df

def convert_data_types_Aramex_Data(df):
    """
    Converts the data types of specified columns in the DataFrame.
    
    Parameters:
        df (pd.DataFrame): The DataFrame whose columns will have their data types converted.
        
    Returns:
        pd.DataFrame: The DataFrame with converted data types.
    """
    # Define a dictionary mapping column names to desired data types
    dtype_dict = {
        'ShprNo': 'object',
        'HAWB': 'object',
        'Delivery_Date': 'datetime64[ns]',
        'CODAmount': 'float64',
        'O_CODAmount': 'float64',
        'Aramex_Inv_date': 'datetime64[ns]',
        'TOKENNO': 'object'
    }
    
    # Convert data types according to the dictionary
    for column, dtype in dtype_dict.items():
        if column in df.columns:
            try:
                df[column] = df[column].astype(dtype)
            except ValueError:
                print(f"Conversion error: Could not convert {column} to {dtype}")
        else:
            print(f"Warning: {column} not in DataFrame")
    
    return df

def convert_data_types_Cosmaline_Data(df):
    """
    Converts the data types of specified columns in the DataFrame.
    
    Parameters:
        df (pd.DataFrame): The DataFrame whose columns will have their data types converted.
        
    Returns:
        pd.DataFrame: The DataFrame with converted data types.
    """
    # Define a dictionary mapping column names to desired data types
    dtype_dict = {
        'Currency': 'object',
        'Driver_Delivery_date': 'datetime64[ns]',
        'Amount': 'float64',
        'OrderNo': 'object'
    }
    
    # Convert data types according to the dictionary
    for column, dtype in dtype_dict.items():
        if column in df.columns:
            try:
                df[column] = df[column].astype(dtype)
            except ValueError:
                print(f"Conversion error: Could not convert {column} to {dtype}")
        else:
            print(f"Warning: {column} not in DataFrame")
    
    return df

def convert_data_types_Credit_Card(df):
    """
    Converts the data types of specified columns in the Credit Card DataFrame.
    
    Parameters:
        df (pd.DataFrame): The DataFrame whose columns will have their data types converted.
        
    Returns:
        pd.DataFrame: The DataFrame with converted data types.
    """
    # Define a dictionary mapping column names to desired data types
    dtype_dict = {
        'Value_date': 'datetime64[ns]',
        'Narrative': 'str',
        'Amount': 'float32',
        'Currency': 'str'
    }
    
    # Convert data types according to the dictionary
    for column, dtype in dtype_dict.items():
        if column in df.columns:
            try:
                df[column] = df[column].astype(dtype)
            except ValueError:
                print(f"Conversion error: Could not convert {column} to {dtype}")
        else:
            print(f"Warning: {column} not in DataFrame")
    
    return df

def clean_tokens_by_date_Aramex_Data(df, date_column, token_column):
    """
    Ensures each unique date has the same token, replacing discrepancies with the mode token for that date.

    Parameters:
        df (pd.DataFrame): The DataFrame containing the data.
        date_column (str): The name of the column containing the dates.
        token_column (str): The name of the column containing the tokens.

    Returns:
        pd.DataFrame: The DataFrame with tokens cleaned.
    """
    def replace_with_mode(group):
        mode_token = group[token_column].mode()[0]  # Calculate the mode token for the group
        group[token_column] = mode_token  # Replace all tokens in the group with the mode token
        return group

    # Group by the date column and apply the replacement function to each group
    df = df.groupby(date_column).apply(replace_with_mode).reset_index(drop=True)
    return df


def update_cod_currency_Aramex_data(df):
    """
    Updates the CODCurrency column based on the CODAmount column.
    If CODAmount is greater than 0, sets CODCurrency to 'LBP'.

    Parameters:
        df (pd.DataFrame): The DataFrame containing CODAmount and CODCurrency columns.

    Returns:
        pd.DataFrame: The DataFrame with updated CODCurrency column.
    """
    df.loc[df['CODAmount'] > 0, 'CODCurrency'] = 'LBP'
    return df

def rename_columns_for_website_ecom_data(df):
    """
    Renames columns of a specific DataFrame based on a predefined dictionary mapping of old names to new names.
    This function is tailored for the 'Website_ECOM_Data' DataFrame.

    Parameters:
        df (pd.DataFrame): The DataFrame whose columns will be renamed.

    Returns:
        pd.DataFrame: The DataFrame with renamed columns.
    """
    # Encapsulated dictionary specific to the Website_ECOM_Data schema
    column_rename_dict = {
        'Order Number': 'order_number',
        'Shipper Name': 'shipper_name',
        'Created At': 'created_at',
        'Delivered At': 'delivered_at',
        'Billing Type': 'billing_type',
        'Amount': 'amount',
        'Currency': 'currency',
        'Country': 'country',
        'AWB': 'AWB'
    }

    return df.rename(columns=column_rename_dict, inplace=False)

def rename_columns_for_Cosmaline_data(df):
    """
    Renames columns of a specific DataFrame based on a predefined dictionary mapping of old names to new names.
    This function is tailored for the 'Website_ECOM_Data' DataFrame.

    Parameters:
        df (pd.DataFrame): The DataFrame whose columns will be renamed.

    Returns:
        pd.DataFrame: The DataFrame with renamed columns.
    """
    # Encapsulated dictionary specific to the Website_ECOM_Data schema
    column_rename_dict = {
        'Driver_Delivery_date': 'Delivery_Date',
        'OrderNo': 'order_number',
        'Amount': 'O_CODAmount',
        'Currency': 'CODCurrency'
    }

    return df.rename(columns=column_rename_dict, inplace=False)


def add_cod_amount_column_Cosmaline_data(df):
    """
    Adds a new column 'CODAmount' to the DataFrame based on the 'CODCurrency' column.
    If 'CODCurrency' is 'USD', 'CODAmount' is set to 0.
    If 'CODCurrency' is 'LBP', 'CODAmount' is set to the value in 'O_CODAmount'.

    Parameters:
        df (pd.DataFrame): The DataFrame to process.

    Returns:
        pd.DataFrame: The DataFrame with the new 'CODAmount' column added.
    """
    df['CODAmount'] = df.apply(
        lambda row: 0 if row['CODCurrency'] == 'USD' else row['O_CODAmount'],
        axis=1
    )
    return df

def add_new_columns_Cosmaline_Data(df):
    """
    Adds three new columns to the DataFrame:
    - 'ShprNo' as blank
    - 'Aramex_Inv_date' as blank
    - 'TOKENNO' as 'Shipped With Cosmaline'

    Parameters:
        df (pd.DataFrame): The DataFrame to process.

    Returns:
        pd.DataFrame: The DataFrame with the new columns added.
    """
    df['ShprNo'] = pd.NA
    df['Aramex_Inv_date'] = pd.NA
    df['TOKENNO'] = 'Shipped With Cosmaline'
    return df


def rename_columns_for_website_Aramex_data(df):
    """
    Renames columns of a specific DataFrame based on a predefined dictionary mapping of old names to new names.
    This function is tailored for the 'Website_ECOM_Data' DataFrame.

    Parameters:
        df (pd.DataFrame): The DataFrame whose columns will be renamed.

    Returns:
        pd.DataFrame: The DataFrame with renamed columns.
    """
    # Encapsulated dictionary specific to the Website_ECOM_Data schema
    column_rename_dict = {
        'HAWB': 'AWB',
    }

    return df.rename(columns=column_rename_dict, inplace=False)

def rename_columns_to_lowercase(df):
    """
    Renames columns of the DataFrame to lowercase in place.

    Parameters:
        df (pd.DataFrame): The DataFrame whose columns will be renamed.

    Returns:
        None
    """
    return df.rename(columns=str.lower, inplace=False)

def add_order_number_Aramex_Data(df, ECOM_df):
    """
    Adds the order_number column from Website_ECOM_Data to ShippedandCollected_Aramex
    based on matching AWB values.

    Parameters:
        ShippedandCollected_Aramex (pd.DataFrame): DataFrame containing shipment and collection data.
        Website_ECOM_Data (pd.DataFrame): DataFrame containing e-commerce data with order numbers.

    Returns:
        pd.DataFrame: Updated DataFrame with the order_number column added.
    """
    # Ensure AWB columns in both DataFrames have the same type
    df['AWB'] = df['AWB'].astype(str)
    ECOM_df['AWB'] = ECOM_df['AWB'].astype(str)
    
    # Merge DataFrames on the 'AWB' column, only adding the 'order_number' column to ShippedandCollected_Aramex
    updated_df = pd.merge(
        df, 
        ECOM_df[['AWB', 'order_number']], 
        on='AWB', 
        how='left'
    )
    return updated_df


def add_AWB_Cosmaline_Data(df, ECOM_df):
    """
    Merges df2 into df1 on the 'order_number' column, including only the 'AWB' column from df2.

    Parameters:
        df1 (pd.DataFrame): The DataFrame to be merged into.
        df2 (pd.DataFrame): The DataFrame containing 'order_number' and 'AWB' columns.

    Returns:
        pd.DataFrame: The resulting DataFrame after the merge.
    """
    updated_df = pd.merge(
        df, 
        ECOM_df[['order_number', 'AWB']], 
        on='order_number', 
        how='left'
    )
    return updated_df

def concatenate_Aramex_Cosmaline(df1, df2):
    """
    Concatenates two DataFrames vertically and resets the index.

    Parameters:
        df1 (pd.DataFrame): The first DataFrame.
        df2 (pd.DataFrame): The second DataFrame.

    Returns:
        pd.DataFrame: The concatenated DataFrame with reset index.
    """
    concatenated_df = pd.concat([df1, df2], ignore_index=True)
    return concatenated_df
    
def convert_data_types_ERP_Data(df):
    """
    Converts the data types of specified columns in the DataFrame.
    
    Parameters:
        df (pd.DataFrame): The DataFrame whose columns will have their data types converted.
        
    Returns:
        pd.DataFrame: The DataFrame with converted data types.
    """
    # Define a dictionary mapping column names to desired data types
    dtype_dict = {
        'CUSTOMER_NUMBER': 'object'
    }
    
    # Convert data types according to the dictionary
    for column, dtype in dtype_dict.items():
        if column in df.columns:
            try:
                df[column] = df[column].astype(dtype)
            except ValueError:
                print(f"Conversion error: Could not convert {column} to {dtype}")
        else:
            print(f"Warning: {column} not in DataFrame")
    
    return df

def convert_data_types_Oracle_Data(df):
    """
    Converts the data types of specified columns in the Oracle Data DataFrame.
    
    Parameters:
        df (pd.DataFrame): The DataFrame whose columns will have their data types converted.
        
    Returns:
        pd.DataFrame: The DataFrame with converted data types.
    """
    dtype_dict = {
        'operating_unit_name': 'object',
        'oracle_reference_order_number': 'int32',
        'ecom_reference_order_number': 'object',
        'order_type': 'object',
        'order_type_name': 'object',
        'ordered_item': 'object',
        'ordered_quantity': 'int32',
        'pricing_quantity_uom': 'object',
        'unit_selling_price': 'float32',
        'unit_list_price': 'float32',
        'ordered_date': 'datetime64[ns]',
        'customer_name': 'object',
        'tax_code': 'object'
    }
    
    for column, dtype in dtype_dict.items():
        if column in df.columns:
            try:
                df[column] = df[column].astype(dtype)
            except ValueError:
                print(f"Conversion error: Could not convert {column} to {dtype}")
        else:
            print(f"Warning: {column} not in DataFrame")
    
    return df


def extract_token_ERP_Data(df, column_name):
    """
    Extracts the token number from the specified column in the DataFrame and updates the column with the extracted token.
    If the token is not found, it keeps the original value.

    Parameters:
    df (pd.DataFrame): The DataFrame containing the column.
    column_name (str): The name of the column from which to extract the token.

    Returns:
    pd.DataFrame: The updated DataFrame with the extracted tokens in the specified column.
    """
    # Define the regex pattern
    pattern = r'Token (\w+)'
    
    # Extract the token using str.extract
    tokens = df[column_name].str.extract(pattern, expand=False)
    
    # Update the column with the extracted tokens only where the pattern is matched
    df[column_name] = tokens.fillna(df[column_name])
    
    return df 

def remove_time_from_datetime_Oracle_Data(df, column_name):
    """
    Removes the time portion from a datetime column in a DataFrame while preserving the datatype as datetime.

    Parameters:
    df (pd.DataFrame): The DataFrame containing the column.
    column_name (str): The name of the column to process.

    Returns:
    pd.DataFrame: The updated DataFrame with the time portion removed from the specified column.
    """
    # Convert the column to datetime format if it's not already
    df[column_name] = pd.to_datetime(df[column_name])
    
    # Remove the time portion and preserve the datatype
    df[column_name] = df[column_name].dt.date
    
    # Convert back to datetime to ensure it remains a datetime object
    df[column_name] = pd.to_datetime(df[column_name])
    
    return df

def rename_columns_for_Oracle_data(df):
    """
    Renames columns of a specific DataFrame based on a predefined dictionary mapping of old names to new names.
    This function is tailored for the 'Website_ECOM_Data' DataFrame.

    Parameters:
        df (pd.DataFrame): The DataFrame whose columns will be renamed.

    Returns:
        pd.DataFrame: The DataFrame with renamed columns.
    """
    # Encapsulated dictionary specific to the Website_ECOM_Data schema
    column_rename_dict = {
    'OPERATING_UNIT_NAME': 'operating_unit_name',
    'ORACLE_REFERENCE_ORDER_NUMBER': 'oracle_reference_order_number',
    'ECOM_REFERENCE_ORDER_NUMBER': 'ecom_reference_order_number',
    'ORDER_TYPE': 'order_type',
    'ORDER_TYPE_NAME': 'order_type_name',
    'ORDERED_ITEM': 'ordered_item',
    'ORDERED_QUANTITY': 'ordered_quantity',
    'PRICING_QUANTITY_UOM': 'pricing_quantity_uom',
    'UNIT_SELLING_PRICE (after discount without vat)': 'unit_selling_price',
    'UNIT_LIST_PRICE( original price before discount without vat)': 'unit_list_price',
    'ORDERED_DATE': 'ordered_date',
    'CUSTOMER_NAME': 'customer_name',
    'TAX_CODE': 'tax_code'
}

    return df.rename(columns=column_rename_dict, inplace=False)

def convert_data_types_aramex_cosmaline(df):
    """
    Converts the data types of specified columns in the Aramex DataFrame to match the database schema.

    Parameters:
        df (pd.DataFrame): The DataFrame whose columns will have their data types converted.
        
    Returns:
        pd.DataFrame: The DataFrame with converted data types.
    """
    dtype_dict = {
        'AWB': 'str',  # Will be converted to varchar(100) in the database
        'ShprNo': 'object',  # Will be converted to varchar(255) in the database
        'Delivery_Date': 'datetime64[ns]',  # Will be converted to datetime in the database
        'CODAmount': 'float32',  # Will be converted to float in the database
        'CODCurrency': 'object',  # Will be converted to varchar(100) in the database
        'O_CODAmount': 'float32',  # Will be converted to float in the database
        'Aramex_Inv_date': 'datetime64[ns]',  # Will be converted to datetime in the database
        'TOKENNO': 'object',  # Will be converted to varchar(255) in the database
        'order_number': 'object'  # Will be converted to varchar(255) in the database
    }
    
    for column, dtype in dtype_dict.items():
        if column in df.columns:
            try:
                df[column] = df[column].astype(dtype)
            except ValueError:
                print(f"Conversion error: Could not convert {column} to {dtype}")
        else:
            print(f"Warning: {column} not in DataFrame")
    
    return df

#for model-------------------------------------------------------------------------------------------------

def convert_strings_to_lists(df):
    df['product_category'] = df['product_category'].apply(lambda x: ast.literal_eval(x))
    return df

def remove_product_not_found(df):
    df = df[df['product_name'] != 'Product Not Found']
    return df

def preprocess_category_list(category_list):
    return [re.sub(r'[.\(\)]', '', category) for category in category_list]

def apply_preprocess_category_list(df):
    df['product_category'] = df['product_category'].apply(preprocess_category_list)
    return df

def convert_lists_to_strings(df):
    df['product_category'] = df['product_category'].apply(lambda x: ', '.join(x) if x else 'Unknown')
    return df

def remove_higher_selling_price(df):
    df = df[df['unit_selling_price'] <= df['unit_list_price']]
    return df

def calculate_total_sales(df):
    df['total_sales_with_discount'] = df['ordered_quantity'] * df['unit_selling_price']
    df['total_sales_without_discount'] = df['ordered_quantity'] * df['unit_list_price']
    return df

def create_relevant_columns_df(df):
    df = df[['ecom_reference_order_number', 'product_name']]
    return df

def remove_shipping_local(df):
    df = df[df['product_name'] != 'SHIPPING_LOCAL']
    return df

def calculate_discount_percentage(df, unit_list_price_col, unit_selling_price_col, new_col_name='discount_percentage'):
    df[new_col_name] = (df[unit_list_price_col] - df[unit_selling_price_col]) / df[unit_list_price_col] * 100
    return df

def expand_column_to_rows(df, column_to_expand, delimiter=', ', new_col_name=None):
    if new_col_name is None:
        new_col_name = column_to_expand
        
def expand_column_to_rows(df, column_to_expand, delimiter=', ', new_col_name=None):
    if new_col_name is None:
        new_col_name = column_to_expand

    df_expanded = df.assign(**{new_col_name: df[column_to_expand].str.split(delimiter)}).explode(new_col_name)
    return df_expanded