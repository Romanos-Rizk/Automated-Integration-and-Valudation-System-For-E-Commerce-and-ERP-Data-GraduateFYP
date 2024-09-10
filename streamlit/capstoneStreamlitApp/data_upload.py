import streamlit as st
import pandas as pd
import ast
import re

# Preprocessing Functions
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

def calculate_discount_percentage(df, unit_list_price_col, unit_selling_price_col, new_col_name='discount_percentage'):
    df[new_col_name] = (df[unit_list_price_col] - df[unit_selling_price_col]) / df[unit_list_price_col] * 100
    return df

def expand_column_to_rows(df, column_to_expand, delimiter=', ', new_col_name=None):
    if new_col_name is None:
        new_col_name = column_to_expand

    df_expanded = df.assign(**{new_col_name: df[column_to_expand].str.split(delimiter)}).explode(new_col_name)
    return df_expanded

def preprocessing_pipeline_for_report(df):
    df = convert_strings_to_lists(df)
    df = remove_product_not_found(df)
    df = apply_preprocess_category_list(df)
    df = convert_lists_to_strings(df)
    df = remove_higher_selling_price(df)
    df = calculate_total_sales(df)
    df = calculate_discount_percentage(df, 'unit_list_price', 'unit_selling_price')
    return df

# Main function for uploading and preprocessing data
def upload_data():
    st.title("Upload Your Data")
    st.markdown("Please upload your dataset (CSV or Excel) to proceed with the analysis.")

    # File uploader widget
    uploaded_file = st.file_uploader("Choose a file", type=['csv', 'xlsx'])

    # Check if a file has been uploaded
    if uploaded_file is not None:
        try:
            # Load CSV or Excel file
            if uploaded_file.name.endswith('.csv'):
                df = pd.read_csv(uploaded_file)
            else:
                df = pd.read_excel(uploaded_file)

            # Apply the preprocessing pipeline
            df = preprocessing_pipeline_for_report(df)
            df_expanded = expand_column_to_rows(df, 'product_category')

            # Display success message and show the first few rows of both dataframes
            st.success("File uploaded and preprocessed successfully!")
            st.write("Processed Data (showing first 5 rows):")
            st.write(df.head())
            st.write("Expanded Data (showing first 5 rows):")
            st.write(df_expanded.head())

            # Return both dataframes
            return df, df_expanded

        except Exception as e:
            # Show error message if something goes wrong during file loading or preprocessing
            st.error(f"Error: {e}")
            return None, None
    else:
        st.warning("Please upload a dataset to proceed.")
        return None, None

