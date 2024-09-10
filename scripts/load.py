import pandas as pd
from sqlalchemy import create_engine
import numpy as np
from sqlalchemy import inspect

def create_db_engine():
    """
    Creates a SQLAlchemy engine with hardcoded credentials.
    
    Returns:
        engine: An SQLAlchemy Engine instance.
    """
    user = 'root'
    password = 'password'  
    host = 'host.docker.internal'
    database = 'CapstoneTest'
    url = f'mysql+pymysql://{user}:{password}@{host}/{database}'
    engine = create_engine(url, echo=True)  # Set echo=False in production for less verbosity
    return engine

def upload_dataframe_to_temp_sql(df, temp_table_name, engine, index=False):
    """
    Uploads a DataFrame to a specified temporary SQL table using SQLAlchemy engine.

    Parameters:
        df (pd.DataFrame): The DataFrame to upload.
        temp_table_name (str): The name of the temporary SQL table to which the data should be uploaded.
        engine: The SQLAlchemy engine object connected to the database.
        index (bool): Whether to write the DataFrame's index as a column in the table.

    Returns:
        None
    """
    try:
        df.to_sql(name=temp_table_name, con=engine, if_exists='replace', index=index)
        print("Data uploaded successfully to the temporary table.")
    except Exception as e:
        print(f"An error occurred while uploading data: {e}")
        
from sqlalchemy import inspect

def merge_data_from_temp_to_main_with_pk(temp_table_name, main_table_name, engine):
    """
    Merges data from a temporary table to the main table.

    Parameters:
        temp_table_name (str): The name of the temporary SQL table.
        main_table_name (str): The name of the main SQL table.
        engine: The SQLAlchemy engine object connected to the database.

    Returns:
        None
    """
    # Inspect the main table to get the column names
    inspector = inspect(engine)
    # Get columns excluding 'id', 'db_entry_time', and 'db_update_time'
    columns = [column['name'] for column in inspector.get_columns(main_table_name) if column['name'] not in ['id', 'db_entry_time', 'db_update_time']]

    # Construct the ON DUPLICATE KEY UPDATE part of the SQL statement
    on_duplicate_key_update = ", ".join([f"{col} = VALUES({col})" for col in columns])

    # Construct the full merge SQL statement
    merge_sql = f"""
    INSERT INTO {main_table_name} ({', '.join(columns)})
    SELECT {', '.join(columns)} FROM {temp_table_name}
    ON DUPLICATE KEY UPDATE
    {on_duplicate_key_update}
    """
    # Execute the merge SQL statement
    with engine.connect() as connection:
        connection.execute(merge_sql)
    print(f"Data from {temp_table_name} merged into {main_table_name} successfully.")

    
def merge_data_from_temp_to_main_without_pk(temp_table_name, main_table_name, engine, columns):
    """
    Merges data from a temporary table to the main table, ensuring no duplicates for tables without a primary key.

    Parameters:
        temp_table_name (str): The name of the temporary SQL table.
        main_table_name (str): The name of the main SQL table.
        engine: The SQLAlchemy engine object connected to the database.
        columns (list): List of columns to compare for duplicates.

    Returns:
        None
    """
    # Exclude 'db_entry_time' and 'db_update_time' from the columns list
    columns = [col for col in columns if col not in ['db_entry_time', 'db_update_time']]

    unique_columns_select = " AND ".join([f"{main_table_name}.{col} = {temp_table_name}.{col}" for col in columns])
    merge_sql = f"""
    INSERT INTO {main_table_name} ({', '.join(columns)})
    SELECT {', '.join([f'{temp_table_name}.{col}' for col in columns])}
    FROM {temp_table_name}
    WHERE NOT EXISTS (
        SELECT 1 
        FROM {main_table_name}
        WHERE {unique_columns_select}
    )
    """
    print(f"Merge SQL:\n{merge_sql}")

    with engine.connect() as connection:
        result = connection.execute(merge_sql)
        print(f"Rows affected: {result.rowcount}")
    print(f"Data from {temp_table_name} merged into {main_table_name} successfully.")

    
def cleanup_temp_tables(engine):
    """
    Drops the temporary tables used during the ETL process.

    Parameters:
        engine: The SQLAlchemy engine object connected to the database.

    Returns:
        None
    """
    temp_tables = [
        'temp_ecom_orders', 
        'temp_aramex_data', 
        'temp_cosmaline_data', 
        'temp_oracle_data', 
        'temp_credit_card_data', 
        'temp_erp_data', 
        'temp_daily_rate',
        'temp_credit_card',
        'temp_ecom_orders',
        'temp_shippedandcollected_aramex_cosmaline',
        'erp_data_tokens',
        'aramex_cod_sum',
        'erp_receipt_sum',
        'erp_cosmaline',
        'aramex_cosmaline_sum',
        'erp_cosmaline_sum',
        'erp_cybersource_sum',
        'credit_card_sum',
        'apriori_temp_results',
        'fpgrowth_temp_results' 
    ]
    
    with engine.connect() as connection:
        for table in temp_tables:
            connection.execute(f"DROP TABLE IF EXISTS {table}")
    print("Temporary tables dropped successfully.")


