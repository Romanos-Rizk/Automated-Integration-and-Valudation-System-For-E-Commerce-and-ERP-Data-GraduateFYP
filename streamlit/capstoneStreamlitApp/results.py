import streamlit as st
import seaborn as sns
import matplotlib.pyplot as plt

def results_page():
    st.title("Results and Visualizations")

    # Load the generated rules from session state
    if 'results' in st.session_state:
        results = st.session_state['results']

        # Scatter Plot of Support and Confidence
        st.subheader("Scatter Plot of Support and Confidence")
        col1, col2 = st.columns([2, 3])  # Adjust column width ratios as needed

        with col1:
            def plot_support_confidence(results):
                fig, ax = plt.subplots(figsize=(8, 5))  # Smaller figure size
                sns.scatterplot(data=results, x='support', y='confidence', size='lift', hue='lift', sizes=(20, 200), palette='viridis', ax=ax)
                ax.set_title('Support vs Confidence')
                ax.set_xlabel('Support')
                ax.set_ylabel('Confidence')
                ax.grid(True)
                st.pyplot(fig)

            plot_support_confidence(results)

        with col2:
            st.markdown(
                """
                <p style='font-size:28px'>
                <br> The 'Scatter Plot of Support and Confidence' visualizes the association rules generated from the dataset. 
                <br><br> Each point represents a rule, with its position determined by the rule's support (x-axis) and confidence (y-axis). 
                The size and color of the points are scaled by the rule's lift, highlighting stronger associations. 
                <br><br> This plot helps in identifying significant rules that may influence business decisions.
                </p>
                """, 
                unsafe_allow_html=True
            )

        # Filter by Antecedents
        st.subheader("Filter Rules by Antecedents")
        all_antecedents = sorted(set(results['antecedents']))
        selected_antecedents = st.multiselect("Select Antecedents:", all_antecedents)

        if selected_antecedents:
            filtered_results = results[results['antecedents'].isin(selected_antecedents)]
        else:
            filtered_results = results

        # Formatted Display of Rules
        st.subheader("Filtered Rules")
        def print_rules(results):
            if results.empty:
                st.write("No rules found with the selected antecedents.")
            else:
                sorted_results = results.sort_values(by='lift', ascending=False)
                for index, row in sorted_results.iterrows():
                    st.markdown(f"### Rule {index + 1}")
                    st.markdown(f"- **Antecedents:** {row['antecedents']}")
                    st.markdown(f"- **Consequents:** {row['consequents']}")
                    st.markdown(f"- **Support:** {row['support']:.3f}")
                    st.markdown(f"- **Confidence:** {row['confidence']:.3f}")
                    st.markdown(f"- **Lift:** {row['lift']:.3f}")
                    st.markdown(f"- **Leverage:** {row.get('leverage', 'N/A'):.3f}")
                    st.markdown(f"- **Conviction:** {row.get('conviction', 'N/A'):.3f}")
                    st.markdown(f"- **Zhang's Metric:** {row.get('zhangs_metric', 'N/A'):.3f}")
                    st.markdown("---")

        print_rules(filtered_results)

    else:
        st.write("Please run the model first to generate results.")
    
    # Footer
    st.markdown("""
    <div style="position: fixed; bottom: 10px; right: 10px; font-size:20px; color:gray;">
        To Malia Group, By AUB Students
    </div>
    """, unsafe_allow_html=True)
