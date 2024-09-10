import pandas as pd
from datetime import datetime
import sys
import os
from sqlalchemy import inspect

from extract import read_data_from_excel, fetch_data_from_db_for_models
from transform import expand_column_to_rows
from transform_pipelines import (concatenate_Aramex_Cosmaline,
                       convert_data_types_aramex_cosmaline,
                       data_preprocessing_pipeline_ECOM_Data, 
                       data_preprocessing_pipeline_Aramex_Data, 
                       data_preprocessing_pipeline_Cosmaline_Data, 
                       data_preprocessing_pipeline_Cosmaline_Aramex_Data,
                       data_preprocessing_pipeline_CreditCard_Data, 
                       data_preprocessing_pipeline_ERP_Data, 
                       data_preprocessing_pipeline_Oracle_Data)
from load import create_db_engine, upload_dataframe_to_temp_sql, merge_data_from_temp_to_main_with_pk, merge_data_from_temp_to_main_without_pk,cleanup_temp_tables
from reconciliation import execute_reconciliation_script
from email_notifications import success_email, failure_email
from models.generate_apriori import generate_apriori_results
from models.generate_fpgrowth import generate_fpgrowth_results
from models.transform_pipelines_for_model import preprocessing_pipeline_for_model, preprocessing_pipeline_for_report
from models.visualizations import generate_all_visualizations
from models.generate_pdf import generate_pdf_report
from models.upload_to_github import upload_files_to_github

def extract_ecom_data():
    df = read_data_from_excel('/opt/airflow/data/Final File.xlsx', sheet_name='Website ECOM Data')
    df.to_csv('/opt/airflow/data/ecom_data.csv', index=False)

def transform_ecom_data():
    df = pd.read_csv('/opt/airflow/data/ecom_data.csv')
    df = data_preprocessing_pipeline_ECOM_Data(df)
    df.to_parquet('/opt/airflow/data/transformed_ecom_data.parquet', index=False)

def load_ecom_data():
    engine = create_db_engine()
    df = pd.read_parquet('/opt/airflow/data/transformed_ecom_data.parquet')
    upload_dataframe_to_temp_sql(df, 'temp_ecom_orders', engine)
    merge_data_from_temp_to_main_with_pk('temp_ecom_orders', 'ECOM_orders', engine)

def extract_aramex_data():
    df = read_data_from_excel('/opt/airflow/data/Final File.xlsx', sheet_name='Shipped & Collected - Aramex')
    df.to_csv('/opt/airflow/data/aramex_data.csv', index=False)

def transform_aramex_data():
    df = pd.read_csv('/opt/airflow/data/aramex_data.csv')
    ecom_data = pd.read_parquet('/opt/airflow/data/transformed_ecom_data.parquet')
    df = data_preprocessing_pipeline_Aramex_Data(df, ecom_data)
    df.to_parquet('/opt/airflow/data/transformed_aramex_data.parquet', index=False)

def extract_cosmaline_data():
    df = read_data_from_excel('/opt/airflow/data/Final File.xlsx', sheet_name='Shipped & Collected - Cosmaline')
    df.to_csv('/opt/airflow/data/cosmaline_data.csv', index=False)

def transform_cosmaline_data():
    df = pd.read_csv('/opt/airflow/data/cosmaline_data.csv')
    ecom_data = pd.read_parquet('/opt/airflow/data/transformed_ecom_data.parquet')
    df = data_preprocessing_pipeline_Cosmaline_Data(df, ecom_data)
    df.to_parquet('/opt/airflow/data/transformed_cosmaline_data.parquet', index=False)

def concatenate_aramex_cosmaline_data():
    aramex_data = pd.read_parquet('/opt/airflow/data/transformed_aramex_data.parquet')
    cosmaline_data = pd.read_parquet('/opt/airflow/data/transformed_cosmaline_data.parquet')
    aramex_data = convert_data_types_aramex_cosmaline(aramex_data)
    cosmaline_data = convert_data_types_aramex_cosmaline(cosmaline_data)
    combined_df = concatenate_Aramex_Cosmaline(aramex_data, cosmaline_data)
    combined_df.to_parquet('/opt/airflow/data/aramex_cosmaline_data.parquet', index=False)

def load_concatenated_data():
    engine = create_db_engine()
    combined_df = pd.read_parquet('/opt/airflow/data/aramex_cosmaline_data.parquet')
    upload_dataframe_to_temp_sql(combined_df, 'temp_shippedandcollected_aramex_cosmaline', engine)
    merge_data_from_temp_to_main_with_pk('temp_shippedandcollected_aramex_cosmaline', 'shippedandcollected_aramex_cosmaline', engine)

def extract_credit_card_data():
    df = read_data_from_excel('/opt/airflow/data/Final File.xlsx', sheet_name='Collected - Credit Card')
    df.to_csv('/opt/airflow/data/credit_card_data.csv', index=False)

def transform_credit_card_data():
    df = pd.read_csv('/opt/airflow/data/credit_card_data.csv')
    df = data_preprocessing_pipeline_CreditCard_Data(df)
    df.to_parquet('/opt/airflow/data/transformed_credit_card_data.parquet', index=False)

def load_credit_card_data():
    engine = create_db_engine()
    df = pd.read_parquet('/opt/airflow/data/transformed_credit_card_data.parquet')
    upload_dataframe_to_temp_sql(df, 'temp_credit_card', engine)
    unique_columns = ['value_date', 'narrative', 'amount', 'currency']
    merge_data_from_temp_to_main_without_pk('temp_credit_card', 'credit_card', engine, unique_columns)

def extract_erp_data():
    df = read_data_from_excel('/opt/airflow/data/Final File.xlsx', sheet_name='ERP-Oracle Collection')
    df.to_csv('/opt/airflow/data/erp_data.csv', index=False)

def transform_erp_data():
    df = pd.read_csv('/opt/airflow/data/erp_data.csv')
    df = data_preprocessing_pipeline_ERP_Data(df)
    df.to_parquet('/opt/airflow/data/transformed_erp_data.parquet', index=False)

def load_erp_data():
    engine = create_db_engine()
    df = pd.read_parquet('/opt/airflow/data/transformed_erp_data.parquet')
    upload_dataframe_to_temp_sql(df, 'temp_erp_data', engine)
    merge_data_from_temp_to_main_with_pk('temp_erp_data', 'erp_data', engine)

def extract_oracle_data():
    df = read_data_from_excel('/opt/airflow/data/Final File.xlsx', sheet_name='Oracle Data')
    df.to_csv('/opt/airflow/data/oracle_data.csv', index=False)

def transform_oracle_data():
    df = pd.read_csv('/opt/airflow/data/oracle_data.csv')
    df = data_preprocessing_pipeline_Oracle_Data(df)
    df.to_parquet('/opt/airflow/data/transformed_oracle_data.parquet', index=False)

def load_oracle_data():
    engine = create_db_engine()
    df = pd.read_parquet('/opt/airflow/data/transformed_oracle_data.parquet')
    upload_dataframe_to_temp_sql(df, 'temp_oracle_data', engine)
    unique_columns = ['operating_unit_name', 'oracle_reference_order_number', 'ecom_reference_order_number', 'order_type', 'order_type_name', 'ordered_item', 'ordered_quantity', 'pricing_quantity_uom', 'unit_selling_price', 'unit_list_price', 'ordered_date', 'customer_name', 'tax_code']
    merge_data_from_temp_to_main_without_pk('temp_oracle_data', 'oracle_data', engine, unique_columns)

def extract_daily_rate_data():
    df = read_data_from_excel('/opt/airflow/data/Final File.xlsx', sheet_name='Daily Rate')
    df.to_csv('/opt/airflow/data/daily_rate_data.csv', index=False)

def load_daily_rate_data():
    engine = create_db_engine()
    df = pd.read_csv('/opt/airflow/data/daily_rate_data.csv')
    df.columns = [col.lower() for col in df.columns]
    upload_dataframe_to_temp_sql(df, 'temp_daily_rate', engine)
    inspector = inspect(engine)
    main_table_columns = [column['name'] for column in inspector.get_columns('daily_rate') if column['name'] != 'id']
    unique_columns = [col for col in df.columns if col in main_table_columns]
    merge_data_from_temp_to_main_without_pk('temp_daily_rate', 'daily_rate', engine, unique_columns)

def execute_cosmaline_reconciliation():
    execute_reconciliation_script('cosmaline_reconciliation.sql')

def execute_credit_card_reconciliaiton():
    execute_reconciliation_script('credit_card_reconciliaiton.sql')

def execute_ecom_orders_not_in_oracle_reconciliation():
    execute_reconciliation_script('ecom_orders_not_in_oracle_reconciliation.sql')

def execute_ecom_orders_not_in_shipping_reconciliation():
    execute_reconciliation_script('ecom_orders_not_in_shipping_reconciliation.sql')

def execute_ecom_reconciliation():
    execute_reconciliation_script('ecom_reconciliation.sql')

def execute_invalid_oracle_order_numbers_reconciliation():
    execute_reconciliation_script('invalid_oracle_order_numbers_reconciliation.sql')

def execute_invalid_shipping_order_numbers_reconciliation():
    execute_reconciliation_script('invalid_shipping_order_numbers_reconciliation.sql')

def execute_token_reconciliation():
    execute_reconciliation_script('token_reconciliation.sql')
    
def cleanup_temp_tables_task():
    engine = create_db_engine()
    cleanup_temp_tables(engine)
    
def extract_data_for_models():
    df = fetch_data_from_db_for_models()
    df.to_csv('/opt/airflow/data/data_for_model.csv')

def transform_data_for_model():
    df = pd.read_csv('/opt/airflow/data/data_for_model.csv')
    df = preprocessing_pipeline_for_model(df)
    df.to_parquet('/opt/airflow/data/data_for_model.parquet', index=False)
    
def generate_and_save_apriori_results():
    df = pd.read_parquet('/opt/airflow/data/data_for_model.parquet')
    apriori_results = generate_apriori_results(df)
    apriori_results.to_parquet('/opt/airflow/data/apriori_results.parquet', index=False)

def generate_and_save_fpgrowth_results():
    df = pd.read_parquet('/opt/airflow/data/data_for_model.parquet')
    fpgrowth_results = generate_fpgrowth_results(df)
    fpgrowth_results.to_parquet('/opt/airflow/data/fpgrowth_results.parquet', index=False)
    
def sanitize_column_names(df):
    """
    Rename DataFrame columns to be valid SQL identifiers by replacing spaces and special characters with underscores.
    """
    df.columns = [col.replace(' ', '_').replace('.', '').lower() for col in df.columns]
    return df

def handle_inf_values(df):
    """
    Replaces inf and -inf values with NaN, and then replaces NaN with 0 in the DataFrame.

    Parameters:
    df (DataFrame): The DataFrame to process.

    Returns:
    DataFrame: The processed DataFrame with inf and NaN values handled.
    """
    # Replace inf and -inf with NaN
    df.replace([float('inf'), float('-inf')], pd.NA, inplace=True)
    # Replace NaN with 0
    df.fillna(0, inplace=True)
    return df
    
def round_float_columns(df, decimal_places=3):
    float_cols = df.select_dtypes(include=['float64']).columns
    df[float_cols] = df[float_cols].round(decimal_places)
    return df

def remove_duplicates(table_name, engine, columns, primary_key='id'):
    column_list = ', '.join(columns)
    # Exclude 'db_entry_time' and 'db_update_time' from duplicate check
    duplicate_check_columns = [col for col in columns if col not in ['db_entry_time', 'db_update_time']]

    delete_duplicates_query = f"""
    DELETE t1 FROM {table_name} t1
    INNER JOIN {table_name} t2 
    WHERE 
        t1.{primary_key} < t2.{primary_key} AND
        { ' AND '.join([f't1.{col} = t2.{col}' for col in duplicate_check_columns]) };
    """

    with engine.connect() as conn:
        conn.execute(delete_duplicates_query)
        conn.execute("COMMIT;")
    
def insert_apriori_results():
    engine = create_db_engine()
    
    # Insert Apriori results
    apriori_results = pd.read_parquet('/opt/airflow/data/apriori_results.parquet')
    apriori_results = sanitize_column_names(apriori_results)
    apriori_results = handle_inf_values(apriori_results)
    apriori_results = round_float_columns(apriori_results, decimal_places=3)
    upload_dataframe_to_temp_sql(apriori_results, 'apriori_temp_results', engine, index=False)
    columns = ['antecedents', 'consequents', 'antecedent_support', 'consequent_support', 'support', 'confidence', 'lift', 'leverage', 'conviction', 'zhangs_metric']
    merge_data_from_temp_to_main_without_pk('apriori_temp_results', 'apriori_results', engine, columns)
    remove_duplicates('apriori_results', engine, columns)

def insert_fpgrowth_results():
    engine = create_db_engine()
    
    # Insert FP-Growth results
    fpgrowth_results = pd.read_parquet('/opt/airflow/data/fpgrowth_results.parquet')
    fpgrowth_results = sanitize_column_names(fpgrowth_results)
    fpgrowth_results = handle_inf_values(fpgrowth_results)
    fpgrowth_results = round_float_columns(fpgrowth_results, decimal_places=3)
    upload_dataframe_to_temp_sql(fpgrowth_results, 'fpgrowth_temp_results', engine, index=False)
    columns = ['antecedents', 'consequents', 'antecedent_support', 'consequent_support', 'support', 'confidence', 'lift', 'leverage', 'conviction', 'zhangs_metric']
    merge_data_from_temp_to_main_without_pk('fpgrowth_temp_results', 'fpgrowth_results', engine, columns)
    remove_duplicates('fpgrowth_results', engine, columns)

#---------------------------------------------------------------Report

def extract_data_for_report():
    df = fetch_data_from_db_for_models()
    df.to_csv('/opt/airflow/scripts/models/data/data_for_report.csv')
    
def transform_data_for_report():
    df = pd.read_csv('/opt/airflow/scripts/models/data/data_for_report.csv')
    df = preprocessing_pipeline_for_report(df)
    df_expanded = expand_column_to_rows(df, 'product_category')
    df.to_parquet('/opt/airflow/scripts/models/data/transformed_data_for_report.parquet', index=False)
    df_expanded.to_parquet('/opt/airflow/scripts/models/data/transformed_data_expanded_for_report.parquet', index=False)
        

def create_pdf_report_task():
    # Define the output directory and the path for the PDF report
    output_dir = '/opt/airflow/scripts/models'
    pdf_path = os.path.join(output_dir, 'sales_report.pdf')

    # Call the function to generate the PDF report
    generate_pdf_report(output_dir, pdf_path)
    print(f"PDF report generated and saved to {pdf_path}")

def generate_visualizations_task():
    output_dir = '/opt/airflow/scripts/models/visuals'
    transformed_data_path = '/opt/airflow/scripts/models/data/transformed_data_for_report.parquet'
    transformed_data_expanded_path = '/opt/airflow/scripts/models/data/transformed_data_expanded_for_report.parquet'
    df = pd.read_parquet(transformed_data_path)
    df_expanded = pd.read_parquet(transformed_data_expanded_path)

    generate_all_visualizations(df, df_expanded, output_dir)
    print(f"Visualizations saved to {output_dir}")

def print_etl_done():
    print("ETL done")
    
#-----------------------------------------------------------------------------------

def upload_files_to_github_task():
    files = {
        "/opt/airflow/scripts/models/data/transformed_data_for_report.parquet": "",
        "/opt/airflow/scripts/models/data/transformed_data_expanded_for_report.parquet": ""
    }
    repo = ""
    token = ''
    message = "Update data files"
    
    upload_files_to_github(files, repo, token, message)


