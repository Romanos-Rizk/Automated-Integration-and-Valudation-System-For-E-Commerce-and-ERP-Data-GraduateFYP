import streamlit as st
from data_upload import upload_data
from eda import eda_page
from model_config import model_configuration_page
from results import results_page

# Main layout of the application
def main():
    st.set_page_config(layout="wide")

    # Display title
    st.title("Market Basket Analysis Application")

    # Step 1: Data Upload
    if 'data' not in st.session_state or 'df_expanded' not in st.session_state:
        st.markdown("### Step 1: Upload Data")
        df, df_expanded = upload_data()
        
        # Once data is uploaded and processed, store it in session state
        if df is not None and df_expanded is not None:
            st.session_state['data'] = df
            st.session_state['df_expanded'] = df_expanded
            st.success("Data uploaded and preprocessed successfully! You can now proceed to the next steps.")
            st.experimental_rerun()  # Forces a rerun to refresh the page with the new state

    # Step 2: Show navigation buttons after data is uploaded
    if 'data' in st.session_state and 'df_expanded' in st.session_state:
        st.markdown("### Step 2: Select an Action")

        # Create navigation buttons
        if st.button("Exploratory Data Analysis"):
            eda_page(st.session_state['data'], st.session_state['df_expanded'])

        if st.button("Model Configuration"):
            model_configuration_page(st.session_state['data'])

        if st.button("Results and Visualizations"):
            results_page()

# Run the main function
if __name__ == "__main__":
    main()
