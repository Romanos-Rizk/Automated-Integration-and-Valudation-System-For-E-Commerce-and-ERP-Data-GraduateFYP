import pandas as pd
import numpy as np

from transform import (convert_strings_to_lists,
                       remove_product_not_found,
                       apply_preprocess_category_list,
                       convert_lists_to_strings,
                       remove_higher_selling_price,
                       calculate_total_sales,
                       create_relevant_columns_df,
                       remove_shipping_local,
                       calculate_discount_percentage)


def preprocessing_pipeline_for_model(df):
    df = convert_strings_to_lists(df)
    df = remove_product_not_found(df)
    df = apply_preprocess_category_list(df)
    df = convert_lists_to_strings(df)
    df = remove_higher_selling_price(df)
    df = calculate_total_sales(df)
    df = create_relevant_columns_df(df)
    df = remove_shipping_local(df)
    return df

def preprocessing_pipeline_for_report(df):
    df = convert_strings_to_lists(df)
    df = remove_product_not_found(df)
    df = apply_preprocess_category_list(df)
    df = convert_lists_to_strings(df)
    df = remove_higher_selling_price(df)
    df = calculate_total_sales(df)
    df = calculate_discount_percentage(df, 'unit_list_price', 'unit_selling_price')
    return df
