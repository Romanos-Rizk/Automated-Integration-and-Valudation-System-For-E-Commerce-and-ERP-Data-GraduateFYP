from mlxtend.frequent_patterns import apriori, association_rules
import pandas as pd

def generate_apriori_results(df):
    # Create a basket matrix (binary matrix)
    basket = (df
              .groupby(['ecom_reference_order_number', 'product_name'])['product_name']
              .count().unstack().reset_index().fillna(0)
              .set_index('ecom_reference_order_number'))

    # Convert quantities to a binary format (purchased or not purchased)
    basket_sets = basket.applymap(lambda x: 1 if x > 0 else 0)

    # Apply the Apriori algorithm
    frequent_itemsets = apriori(basket_sets, min_support=0.01, use_colnames=True)

    # Generate association rules
    rules = association_rules(frequent_itemsets, metric="lift", min_threshold=1)

    # Filter rules based on support, confidence, and lift
    filtered_rules = rules[(rules['support'] >= 0.01) & 
                           (rules['confidence'] >= 0.5) & 
                           (rules['lift'] > 1)]
    
    # Convert frozenset to string
    filtered_rules['antecedents'] = filtered_rules['antecedents'].apply(lambda x: ', '.join(list(x)))
    filtered_rules['consequents'] = filtered_rules['consequents'].apply(lambda x: ', '.join(list(x)))

    # Ensure data types match the MySQL table schema
    filtered_rules['support'] = filtered_rules['support'].astype(float)
    filtered_rules['confidence'] = filtered_rules['confidence'].astype(float)
    filtered_rules['lift'] = filtered_rules['lift'].astype(float)

    return filtered_rules
