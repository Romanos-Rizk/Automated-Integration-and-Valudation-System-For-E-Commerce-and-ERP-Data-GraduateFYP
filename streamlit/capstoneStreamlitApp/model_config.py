import streamlit as st
from mlxtend.frequent_patterns import apriori, fpgrowth, association_rules
import pandas as pd

def model_configuration_page(data):
    st.title("Model Configuration")

    # Introduction and Explanation
    st.subheader("Introduction to Market Basket Analysis")
    st.markdown("""
    Market Basket Analysis is a technique used to identify associations or relationships between products. It uses two primary algorithms: **Apriori** and **FP-Growth**.

    ### Key Metrics:
    - **Support:** The proportion of transactions in the dataset that contain the itemset.
    - **Confidence:** A measure of the reliability of the rule. It's calculated as the ratio of the number of transactions containing the itemset to the number of transactions containing the antecedent.
    - **Lift:** The ratio of the observed support to that expected if the two itemsets were independent. A lift greater than 1 indicates a strong association between the antecedent and the consequent.
    """)

    # Model Selection
    st.subheader("Select Model and Set Parameters")
    model_choice = st.radio("Choose the algorithm:", ("Apriori", "FP-Growth"))

    # Common Parameters
    st.markdown("### Common Parameters")
    min_support = st.slider("Minimum Support:", min_value=0.01, max_value=1.0, value=0.02, step=0.01)
    min_confidence = st.slider("Minimum Confidence:", min_value=0.1, max_value=1.0, value=0.7, step=0.1)
    min_lift = st.slider("Minimum Lift:", min_value=1.0, max_value=20.0, value=10.0, step=0.5)

    # Function to run the chosen model
    def run_model(df, model_choice, min_support, min_confidence, min_lift):
        basket = (df
                .groupby(['ecom_reference_order_number', 'product_name'])['product_name']
                .count().unstack().reset_index().fillna(0)
                .set_index('ecom_reference_order_number'))
        basket_sets = basket.applymap(lambda x: 1 if x > 0 else 0)

        if model_choice == "Apriori":
            frequent_itemsets = apriori(basket_sets, min_support=min_support, use_colnames=True)
        elif model_choice == "FP-Growth":
            frequent_itemsets = fpgrowth(basket_sets, min_support=min_support, use_colnames=True)
        
        if frequent_itemsets.empty:
            return None
        
        rules = association_rules(frequent_itemsets, metric="confidence", min_threshold=min_confidence)
        filtered_rules = rules[(rules['lift'] >= min_lift)]
        filtered_rules['antecedents'] = filtered_rules['antecedents'].apply(lambda x: ', '.join(list(x)))
        filtered_rules['consequents'] = filtered_rules['consequents'].apply(lambda x: ', '.join(list(x)))
        filtered_rules.reset_index(drop=True, inplace=True)  # Reset the index
        return filtered_rules

    # Run Model Button
    if st.button("Run Model"):
        with st.spinner("Running the model..."):
            result = run_model(data, model_choice, min_support, min_confidence, min_lift)
            if result is None or result.empty:
                st.error("No rules found with the given combination of parameters. Please adjust the thresholds.")
            else:
                st.success(f"Model run successfully! Number of rules: {len(result)}")
                st.write(result)
                st.session_state['results'] = result  # Store results in session state

    # Footer
    st.markdown("""
    <div style="position: fixed; bottom: 10px; right: 10px; font-size:20px; color:gray;">
        To Malia Group, By AUB Students
    </div>
    """, unsafe_allow_html=True)
