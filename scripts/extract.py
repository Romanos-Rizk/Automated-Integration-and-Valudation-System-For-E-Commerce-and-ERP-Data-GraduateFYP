import pandas as pd
import numpy as np
from load import create_db_engine
from sqlalchemy import create_engine, Table, Column, String, MetaData, select

def read_data_from_excel(filepath, sheet_name='Website ECOM Data'):
    """
    Reads data from an Excel file into a Pandas DataFrame.

    Parameters:
        filepath (str): The path to the Excel file.
        sheet_name (str or int): The sheet name or index to read data from. Defaults to the first sheet.

    Returns:
        pd.DataFrame: The data read from the Excel file.
    """
    try:
        df = pd.read_excel(filepath, sheet_name=sheet_name)
        #print("Data read successfully from Excel.")
        return df
    except Exception as e:
        print(f"An error occurred while reading the data: {e}")
        return None
    

def fetch_data_from_db_for_models():
    engine = create_db_engine()
    connection = engine.connect()

    query = """
    SELECT 
        oracle_data.operating_unit_name,
        oracle_data.ecom_reference_order_number,
        oracle_data.ordered_item,
        oracle_data.ordered_quantity,
        oracle_data.unit_selling_price,
        oracle_data.unit_list_price,
        oracle_data.ordered_date,
        oracle_data.tax_code,
        oracle_data_product_name.product_id,
        oracle_data_product_name.product_name,
        oracle_data_product_name.product_category
    FROM 
        oracle_data
    JOIN 
        oracle_data_product_name 
    ON 
        oracle_data.ordered_item = oracle_data_product_name.product_id;
    """

    result = connection.execute(query)
    df = pd.DataFrame(result.fetchall(), columns=result.keys())

    connection.close()
    return df