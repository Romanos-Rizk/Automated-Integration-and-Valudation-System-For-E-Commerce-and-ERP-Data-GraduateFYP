import pandas as pd
from airflow import DAG
from airflow.utils.email import send_email
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator
import sys
import os
from sqlalchemy import inspect

# Add the scripts directory to the system path
sys.path.append('/opt/airflow/scripts')

# Import your ETL functions
from extract import read_data_from_excel
from etl_functions import(extract_ecom_data,
                          transform_ecom_data,
                          load_ecom_data,
                          extract_aramex_data,
                          transform_aramex_data,
                          extract_cosmaline_data,
                          transform_cosmaline_data,
                          concatenate_aramex_cosmaline_data,
                          load_concatenated_data,
                          extract_credit_card_data,
                          transform_credit_card_data,
                          load_credit_card_data,
                          extract_erp_data,
                          transform_erp_data,
                          load_erp_data,
                          extract_oracle_data,
                          transform_oracle_data,
                          load_oracle_data,
                          extract_daily_rate_data,
                          load_daily_rate_data,
                          execute_cosmaline_reconciliation,
                          execute_credit_card_reconciliaiton,
                          execute_ecom_orders_not_in_oracle_reconciliation,
                          execute_ecom_orders_not_in_shipping_reconciliation,
                          execute_ecom_reconciliation,
                          execute_invalid_oracle_order_numbers_reconciliation,
                          execute_invalid_shipping_order_numbers_reconciliation,
                          execute_token_reconciliation,
                          cleanup_temp_tables_task, 
                          extract_data_for_models, 
                          transform_data_for_model, 
                          generate_and_save_apriori_results, 
                          generate_and_save_fpgrowth_results,
                          insert_apriori_results,
                          insert_fpgrowth_results,
                          extract_data_for_report,
                          transform_data_for_report,
                          generate_visualizations_task,
                          create_pdf_report_task,
                          print_etl_done,
                          upload_files_to_github_task
)
from load import create_db_engine, upload_dataframe_to_temp_sql, merge_data_from_temp_to_main_with_pk, merge_data_from_temp_to_main_without_pk,cleanup_temp_tables
from reconciliation import execute_reconciliation_script
from email_notifications import success_email, failure_email, send_sales_report_email

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'email': ['rar37@mail.aub.edu', 'hmf31@mail.aub.edu'],
    'email_on_failure': False,
    'email_on_retry':False,
    'email_on_success': False,
    'on_failure_callback': failure_email,
}

# Create the DAG
dag = DAG(
    'etl_dag',
    default_args=default_args,
    description='ETL DAG for Capstone Project',
    schedule_interval='@daily'
)

# Create PythonOperator tasks for each step

extract_ecom_task = PythonOperator(
    task_id='extract_ecom_data',
    python_callable=extract_ecom_data,
    dag=dag,
)

transform_ecom_task = PythonOperator(
    task_id='transform_ecom_data',
    python_callable=transform_ecom_data,
    dag=dag,
)

load_ecom_task = PythonOperator(
    task_id='load_ecom_data',
    python_callable=load_ecom_data,
    dag=dag,
)

extract_aramex_task = PythonOperator(
    task_id='extract_aramex_data',
    python_callable=extract_aramex_data,
    dag=dag,
)

transform_aramex_task = PythonOperator(
    task_id='transform_aramex_data',
    python_callable=transform_aramex_data,
    dag=dag,
)

extract_cosmaline_task = PythonOperator(
    task_id='extract_cosmaline_data',
    python_callable=extract_cosmaline_data,
    dag=dag,
)

transform_cosmaline_task = PythonOperator(
    task_id='transform_cosmaline_data',
    python_callable=transform_cosmaline_data,
    dag=dag,
)

concatenate_aramex_cosmaline_task = PythonOperator(
    task_id='concatenate_aramex_cosmaline_data',
    python_callable=concatenate_aramex_cosmaline_data,
    dag=dag,
)

load_concatenated_task = PythonOperator(
    task_id='load_concatenated_data',
    python_callable=load_concatenated_data,
    dag=dag,
)

extract_credit_card_task = PythonOperator(
    task_id='extract_credit_card_data',
    python_callable=extract_credit_card_data,
    dag=dag,
)

transform_credit_card_task = PythonOperator(
    task_id='transform_credit_card_data',
    python_callable=transform_credit_card_data,
    dag=dag,
)

load_credit_card_task = PythonOperator(
    task_id='load_credit_card_data',
    python_callable=load_credit_card_data,
    dag=dag,
)

extract_erp_task = PythonOperator(
    task_id='extract_erp_data',
    python_callable=extract_erp_data,
    dag=dag,
)

transform_erp_task = PythonOperator(
    task_id='transform_erp_data',
    python_callable=transform_erp_data,
    dag=dag,
)

load_erp_task = PythonOperator(
    task_id='load_erp_data',
    python_callable=load_erp_data,
    dag=dag,
)

extract_oracle_task = PythonOperator(
    task_id='extract_oracle_data',
    python_callable=extract_oracle_data,
    dag=dag,
)

transform_oracle_task = PythonOperator(
    task_id='transform_oracle_data',
    python_callable=transform_oracle_data,
    dag=dag,
)

load_oracle_task = PythonOperator(
    task_id='load_oracle_data',
    python_callable=load_oracle_data,
    dag=dag,
)

extract_daily_rate_task = PythonOperator(
    task_id='extract_daily_rate_data',
    python_callable=extract_daily_rate_data,
    dag=dag,
)

load_daily_rate_task = PythonOperator(
    task_id='load_daily_rate_data',
    python_callable=load_daily_rate_data,
    dag=dag,
)

all_data_loaded = PythonOperator(
    task_id='all_data_loaded',
    python_callable=lambda: print("All data loading tasks are completed."),
    dag=dag,
)

token_reconciliation_task = PythonOperator(
    task_id='execute_token_reconciliation',
    python_callable=execute_token_reconciliation,
    dag=dag,
)

cosmaline_reconciliation_task = PythonOperator(
    task_id='execute_cosmaline_reconciliation',
    python_callable=execute_cosmaline_reconciliation,
    dag=dag,
)

ecom_reconciliation_task = PythonOperator(
    task_id='execute_ecom_reconciliation',
    python_callable=execute_ecom_reconciliation,
    dag=dag,
)

credit_card_reconciliation_task = PythonOperator(
    task_id='execute_credit_card_reconciliaiton',
    python_callable=execute_credit_card_reconciliaiton,
    dag=dag,
)

ecom_orders_not_in_shipping_task = PythonOperator(
    task_id='execute_ecom_orders_not_in_shipping_reconciliation',
    python_callable=execute_ecom_orders_not_in_shipping_reconciliation,
    dag=dag,
)

ecom_orders_not_in_oracle_task = PythonOperator(
    task_id='execute_ecom_orders_not_in_oracle_reconciliation',
    python_callable=execute_ecom_orders_not_in_oracle_reconciliation,
    dag=dag,
)

invalid_shipping_order_numbers_task = PythonOperator(
    task_id='execute_invalid_shipping_order_numbers_reconciliation',
    python_callable=execute_invalid_shipping_order_numbers_reconciliation,
    dag=dag,
)

invalid_oracle_order_numbers_task = PythonOperator(
    task_id='execute_invalid_oracle_order_numbers_reconciliation',
    python_callable=execute_invalid_oracle_order_numbers_reconciliation,
    dag=dag,
)

cleanup_task = PythonOperator(
    task_id='cleanup_temp_tables',
    python_callable=cleanup_temp_tables_task,
    dag=dag,
)

# Task to extract data from the database and save it to CSV
extract_for_model = PythonOperator(
    task_id='extract_data_for_models',
    python_callable=extract_data_for_models,
    dag=dag,
)

# Task to transform the data and save it to a Parquet file
transform_for_model = PythonOperator(
    task_id='transform_data_for_model',
    python_callable=transform_data_for_model,
    dag=dag,
)

# Task to generate and save Apriori model results
generate_apriori_model = PythonOperator(
    task_id='generate_and_save_apriori_results',
    python_callable=generate_and_save_apriori_results,
    dag=dag,
)

# Task to generate and save FP-Growth model results
generate_fpgrowth_model = PythonOperator(
    task_id='generate_and_save_fpgrowth_results',
    python_callable=generate_and_save_fpgrowth_results,
    dag=dag,
)

# Task to insert model results into the database
load_model_apriori_results = PythonOperator(
    task_id='insert_model_apriori_results',
    python_callable=insert_apriori_results,
    dag=dag,
)

# Task to insert model results into the database
load_model_fpgrowth_results = PythonOperator(
    task_id='insert_model_fpgrowth_results',
    python_callable=insert_fpgrowth_results,
    dag=dag,
)


# Define a dummy operator to send a success email at the end of the DAG
all_completed = DummyOperator(
    task_id='all_completed',
    dag=dag,
    trigger_rule='all_success',  # This task will be executed only if all upstream tasks succeed
    on_success_callback=success_email,  # Success email callback for the final task
)

print_etl_done_task = PythonOperator(
    task_id='etl_done',
    python_callable=print_etl_done,
    dag=dag,
)


extract_task_for_report = PythonOperator(
    task_id='extract_data_for_report',
    python_callable=extract_data_for_report,
    dag=dag,
)

transform_task_for_report = PythonOperator(
    task_id='transform_data_for_report',
    python_callable=transform_data_for_report,
    dag=dag,
)

visualize_task_for_report = PythonOperator(
    task_id='generate_visualizations_task',
    python_callable=generate_visualizations_task,
    dag=dag,
)

create_pdf_task_for_report = PythonOperator(
    task_id='create_pdf_report_task',
    python_callable=create_pdf_report_task,
    dag=dag,
)

send_report_task = PythonOperator(
    task_id='send_report_task',
    python_callable=send_sales_report_email,
    op_args=['/opt/airflow/scripts/models/sales_report.pdf'],
    dag=dag
)

upload_files_task = PythonOperator(
    task_id='upload_files_to_github',
    python_callable=upload_files_to_github_task,
    dag=dag
)
# Define task dependencies

# ECOM tasks
extract_ecom_task >> transform_ecom_task >> load_ecom_task

# Cosmaline, Aramex, and Oracle tasks
extract_cosmaline_task >> transform_cosmaline_task
extract_aramex_task >> transform_aramex_task
extract_oracle_task >> transform_oracle_task >> load_oracle_task

# Transform tasks for Cosmaline and Aramex dependent on ECOM transformation
transform_ecom_task >> transform_cosmaline_task
transform_ecom_task >> transform_aramex_task

# Concatenate tasks
transform_cosmaline_task >> concatenate_aramex_cosmaline_task
transform_aramex_task >> concatenate_aramex_cosmaline_task

# Load Cosmaline and Aramex tasks are dependent on ECOM data load
load_ecom_task >> load_concatenated_task
load_ecom_task >> load_oracle_task

# Concatenate and load concatenated data
concatenate_aramex_cosmaline_task >> load_concatenated_task

# ERP, Credit Card, and Daily Rate tasks (independent of others)
extract_erp_task >> transform_erp_task >> load_erp_task
extract_credit_card_task >> transform_credit_card_task >> load_credit_card_task
extract_daily_rate_task >> load_daily_rate_task

# All data loading tasks must complete before the intermediary task
[load_ecom_task, load_concatenated_task, load_oracle_task, load_erp_task, load_credit_card_task, load_daily_rate_task] >> all_data_loaded

# Intermediary task must complete before any reconciliation tasks start
all_data_loaded >> token_reconciliation_task
all_data_loaded >> cosmaline_reconciliation_task
all_data_loaded >> ecom_reconciliation_task
all_data_loaded >> credit_card_reconciliation_task
all_data_loaded >> ecom_orders_not_in_shipping_task
all_data_loaded >> ecom_orders_not_in_oracle_task
all_data_loaded >> invalid_shipping_order_numbers_task
all_data_loaded >> invalid_oracle_order_numbers_task

# Task dependencies to ensure cleanup_task is done before starting model-related tasks
[token_reconciliation_task, cosmaline_reconciliation_task, ecom_reconciliation_task, credit_card_reconciliation_task, ecom_orders_not_in_shipping_task, ecom_orders_not_in_oracle_task, invalid_shipping_order_numbers_task, invalid_oracle_order_numbers_task] >> print_etl_done_task

# Model-related tasks start after cleanup_task
print_etl_done_task >> extract_for_model
extract_for_model >> transform_for_model
transform_for_model >> [generate_apriori_model, generate_fpgrowth_model]
generate_apriori_model >> load_model_apriori_results
generate_fpgrowth_model >> load_model_fpgrowth_results

#Report
print_etl_done_task >> extract_task_for_report >> transform_task_for_report >> visualize_task_for_report >> create_pdf_task_for_report >> send_report_task

# All tasks must complete before all_completed
[load_model_apriori_results, load_model_fpgrowth_results, send_report_task] >> cleanup_task
transform_task_for_report >> upload_files_task >> cleanup_task
cleanup_task >> all_completed