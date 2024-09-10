import pandas as pd
import numpy as np

from transform import (lower_columns, 
                       uppercase_columns, 
                       remove_missing_values, 
                       remove_duplicates, 
                       convert_data_types_ECOM_Data, 
                       convert_data_types_Aramex_Data, 
                       convert_data_types_Cosmaline_Data, 
                       clean_tokens_by_date_Aramex_Data, 
                       update_cod_currency_Aramex_data, 
                       rename_columns_for_website_ecom_data, 
                       rename_columns_for_Cosmaline_data, 
                       add_cod_amount_column_Cosmaline_data, 
                       add_new_columns_Cosmaline_Data, 
                       rename_columns_for_website_Aramex_data, 
                       add_order_number_Aramex_Data, 
                       add_AWB_Cosmaline_Data, 
                       concatenate_Aramex_Cosmaline, 
                       convert_data_types_ERP_Data, 
                       convert_data_types_Oracle_Data, 
                       extract_token_ERP_Data, 
                       remove_time_from_datetime_Oracle_Data, 
                       rename_columns_for_Oracle_data,
                       convert_data_types_Credit_Card,
                       rename_columns_to_lowercase,
                       convert_data_types_aramex_cosmaline)


def data_preprocessing_pipeline_ECOM_Data(df):
    """
    Process a DataFrame through a series of cleaning and normalization steps.

    Parameters:
        df (pd.DataFrame): The DataFrame to be processed.
        
    Returns:
        pd.DataFrame: The processed DataFrame.
    """
    # Convert Data types
    df = convert_data_types_ECOM_Data(df)
    
    # Normalize some columns to lower case
    df = lower_columns(df, ['Shipper Name', 'Billing Type'])
    
    # Normalize some columns to upper case
    df = uppercase_columns(df, ['Currency', 'Country'])
    
    # Remove rows with missing values
    df = remove_missing_values(df)
    
    # Remove duplicate rows, keeping the first occurrence
    df = remove_duplicates(df)
    
    # Change Column Names
    df = rename_columns_for_website_ecom_data(df)
    
    return df

def data_preprocessing_pipeline_Aramex_Data(df, ECOM_df):
    """
    Process a DataFrame through a series of cleaning and normalization steps.

    Parameters:
        df (pd.DataFrame): The DataFrame to be processed.
        
    Returns:
        pd.DataFrame: The processed DataFrame.
    """
    # Convert Data types
    df = convert_data_types_Aramex_Data(df)
    
    # Standardize the HAWB COlumn
    #df = clean_HAWB_column_Aramex_Data(df, 'HAWB')
    
    # Rename the HAWB columns
    df = rename_columns_for_website_Aramex_data(df)
    
    # Normalize some columns to upper case
    df = uppercase_columns(df, ['CODCurrency'])
    
    # Update CODCurrency
    df = update_cod_currency_Aramex_data(df)
    
    # Cleaning the TOKEN column to have the same TOken per day
    df = clean_tokens_by_date_Aramex_Data(df, 'Aramex_Inv_date', 'TOKENNO')
    
    # Adding the order_number column from the ECOm data
    df = add_order_number_Aramex_Data(df, ECOM_df)
    
    # Converting data types:
    df = convert_data_types_aramex_cosmaline(df)
    
    
    return df


def data_preprocessing_pipeline_Cosmaline_Data(df, ECOM_df):
    """
    Process a DataFrame through a series of cleaning and normalization steps.

    Parameters:
        df (pd.DataFrame): The DataFrame to be processed.
        
    Returns:
        pd.DataFrame: The processed DataFrame.
    """
    # Convert Data types
    df = convert_data_types_Cosmaline_Data(df)
    
    # Normalize some columns to upper case
    df = uppercase_columns(df, ['Currency'])
    
    # Renaming the columns:
    df = rename_columns_for_Cosmaline_data(df)
    
    # Adding CODAmount:
    df = add_cod_amount_column_Cosmaline_data(df)
    
    # Adding new columns:
    df = add_new_columns_Cosmaline_Data(df)
    
    # Adding the AWB from ECOM data:
    df = add_AWB_Cosmaline_Data(df, ECOM_df)
    
    # Converting data types:
    df = convert_data_types_aramex_cosmaline(df)

    
    return df

def data_preprocessing_pipeline_Cosmaline_Aramex_Data(df):
    """
    Process a DataFrame through a series of cleaning and normalization steps.

    Parameters:
        df (pd.DataFrame): The DataFrame to be processed.
        
    Returns:
        pd.DataFrame: The processed DataFrame.
    """
    # Remove Duplicates
    df = remove_duplicates(df)
    
    return df

def data_preprocessing_pipeline_CreditCard_Data(df):
    """
    Process a DataFrame through a series of cleaning and normalization steps.

    Parameters:
        df (pd.DataFrame): The DataFrame to be processed.
        
    Returns:
        pd.DataFrame: The processed DataFrame.
    """
    # Upper case the Narrative column and Currency
    df = uppercase_columns(df, ['Narrative', 'Currency'])
    
    # COnverting the Data types:
    df = convert_data_types_Credit_Card(df)
    
    # Lowecasing Column Names:
    df = rename_columns_to_lowercase(df)
    
    
    return df

def data_preprocessing_pipeline_ERP_Data(df):
    """
    Process a DataFrame through a series of cleaning and normalization steps.

    Parameters:
        df (pd.DataFrame): The DataFrame to be processed.
        
    Returns:
        pd.DataFrame: The processed DataFrame.
    """
    # Convertin the Customer Number to object
    df = convert_data_types_ERP_Data(df)
    
    # Conveting some columns to upper case:
    df = uppercase_columns(df, ['CURRENCY_CODE'])
    
    # COnverting some columns to lower case:
    df = lower_columns(df, ['CUSTOMER_NAME','RECEIPT_CLASS'])
    
    # Extracting the Token from the Comments columns:
    df = extract_token_ERP_Data(df, 'COMMENTS')

    
    return df


def data_preprocessing_pipeline_Oracle_Data(df):
    """
    Process a DataFrame through a series of cleaning and normalization steps.

    Parameters:
        df (pd.DataFrame): The DataFrame to be processed.
        
    Returns:
        pd.DataFrame: The processed DataFrame.
    """

    # Removing the hours from the date column:
    df = remove_time_from_datetime_Oracle_Data(df, 'ORDERED_DATE')
    
    # Renaming the Columns:
    df = rename_columns_for_Oracle_data(df)
    
    # Convertin the ECOM ref order number to object
    df = convert_data_types_Oracle_Data(df)
    
    
    return df