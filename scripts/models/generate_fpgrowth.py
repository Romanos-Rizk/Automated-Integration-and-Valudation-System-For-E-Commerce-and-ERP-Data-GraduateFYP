from mlxtend.frequent_patterns import fpgrowth, association_rules
import pandas as pd

def generate_fpgrowth_results(df):
    # Prepare the data
    basket = (df
              .groupby(['ecom_reference_order_number', 'product_name'])['product_name']
              .count().unstack().reset_index().fillna(0)
              .set_index('ecom_reference_order_number'))
    basket_sets = basket.applymap(lambda x: 1 if x > 0 else 0)

    # Apply the FP-Growth algorithm
    min_support_threshold = 0.005
    frequent_itemsets = fpgrowth(basket_sets, min_support=min_support_threshold, use_colnames=True)

    # Generate association rules
    min_confidence_threshold = 0.5
    rules = association_rules(frequent_itemsets, metric="confidence", min_threshold=min_confidence_threshold)

    # Filter rules based on lift
    filtered_rules = rules[rules['lift'] > 1]
    
    # Convert frozenset to string
    filtered_rules['antecedents'] = filtered_rules['antecedents'].apply(lambda x: ', '.join(list(x)))
    filtered_rules['consequents'] = filtered_rules['consequents'].apply(lambda x: ', '.join(list(x)))

    # Ensure data types match the MySQL table schema
    filtered_rules['support'] = filtered_rules['support'].astype(float)
    filtered_rules['confidence'] = filtered_rules['confidence'].astype(float)
    filtered_rules['lift'] = filtered_rules['lift'].astype(float)

    return filtered_rules
