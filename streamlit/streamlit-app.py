import pandas as pd
import ast
import re
import matplotlib.pyplot as plt
import seaborn as sns
import squarify
import streamlit as st
import seaborn as sns
from mlxtend.frequent_patterns import apriori, fpgrowth, association_rules
from PIL import Image
import requests
from io import BytesIO
import plotly.express as px

# Set the page layout to wide mode
st.set_page_config(layout="wide")

# Local file path for the image
#coverImage_path = "C:/Users/Lenovo/AIRFLOW_DOCKER_1/streamlit/resources/5BannerImage.jpg"
coverImage_path = "https://raw.githubusercontent.com/Romanos-Rizk/AUB-capstone/main/resources/5BannerImage.jpg"

# Fetch the image data from the URL
response = requests.get(coverImage_path)
coverImage = Image.open(BytesIO(response.content))
# Open the image file directly
#coverImage = Image.open(coverImage_path)

# Example credentials (replace with more secure storage in production)
credentials = {
    "romanos": "romanos",
    "user2": "mypassword",
}

# Initialize session state
if "logged_in" not in st.session_state:
    st.session_state["logged_in"] = False

if "current_page" not in st.session_state:
    st.session_state["current_page"] = "Login"

def login_page():
    # Add some padding to the top to centralize the content vertically
    st.markdown(
        """
        <style>
        .centered-content {
            padding-top: 150px;  /* Adjust this value to control the amount of white space at the top */
        }
        </style>
        """,
        unsafe_allow_html=True
    )

    # Create two columns with a 2/3 and 1/3 ratio
    col1, col2 = st.columns([2, 1])

    # Display the company image in the left column, covering the entire column
    with col1:
        st.markdown('<div class="centered-content">', unsafe_allow_html=True)  # Add the padding CSS
        st.image(coverImage, use_column_width=True)

    # Display the login form in the right column with additional padding
    with col2:
        st.markdown('<div class="centered-content">', unsafe_allow_html=True)  # Add the padding CSS
        st.title("Login")

        # Username and Password input with unique keys
        username = st.text_input("Username", key="login_username")
        password = st.text_input("Password", type="password", key="login_password")

        # if st.button("Login"):
        #     if credentials.get(username) == password:
        #         st.session_state["logged_in"] = True
        #         st.session_state["current_page"] = "Upload"
        #         st.experimental_rerun()  # Force a rerun to redirect immediately
        #     else:
        #         st.error("Invalid username or password")

        # st.markdown('</div>', unsafe_allow_html=True)  # Close the centered content div
        
        if st.button("Login"):
            if credentials.get(username) == password:
                st.session_state["logged_in"] = True
                st.session_state["current_page"] = "Upload"
            else:
                st.error("Invalid username or password")

        # # Redirect to the correct page based on session state
        # if st.session_state.get("logged_in"):
        #     if st.session_state.get("current_page") == "Upload":
        #         upload_page()  # Render the Upload page if logged in and ready
        #     elif st.session_state.get("current_page") == "Home":
        #         home_page()  # Render Home page after file upload is complete
        # else:
        #     # If not logged in, stay on the login page
        #     login_page()
       

#----------------------------------------------------------------------------------------------------------

# Preprocessing functions provided by you
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

def upload_page():
    # Create three columns with a 1/3, 0.05 for divider, and 2/3 ratio
    col1, divider, col2 = st.columns([1, 0.05, 2])

    # Left Column: File Upload (1/3)
    with col1:
        st.markdown("<h1 style='color: #007bff;'>Upload Your Data</h1>", unsafe_allow_html=True)

        file = st.file_uploader(
            "Please upload a CSV or Excel file",
            type=["csv", "xlsx"],
            help="Make sure your file includes the required columns for Market Basket Analysis."
        )

        # Preprocessing and File Handling inside the first column
        if file is not None:
            try:
                # Read the uploaded file into a DataFrame
                if file.name.endswith('.csv'):
                    df = pd.read_csv(file)
                else:
                    df = pd.read_excel(file)

                st.success("File uploaded successfully!")

                # Apply preprocessing pipeline
                data = preprocessing_pipeline_for_report(df)

                # Create the expanded DataFrame
                data_expanded = expand_column_to_rows(data, 'product_category')

                st.write("Preprocessing complete! Here's a preview of your data:")
                st.write(data.head())

                # Save 'data' and 'data_expanded' to session state so they can be used in other pages
                st.session_state['data'] = data
                st.session_state['data_expanded'] = data_expanded

                # Apply styling to make the "Proceed to Home Page" button larger
                st.markdown(
                    """
                    <style>
                    .big-button {
                        display: inline-block;
                        padding: 1rem 2rem;
                        font-size: 1.2rem;
                        font-weight: bold;
                        color: white;
                        background-color: #4CAF50;
                        border: none;
                        border-radius: 10px;
                        cursor: pointer;
                        text-align: center;
                    }
                    .big-button:hover {
                        background-color: #45a049;
                    }
                    </style>
                    """, unsafe_allow_html=True
                )

            #     # Use Streamlit's st.button with CSS styling applied
            #     if st.button("Proceed to Home Page", key="proceed_button"):
            #         # Set both session states to ensure the flow moves forward
            #         st.session_state["data_uploaded"] = True
            #         st.session_state["current_page"] = "Home"
            #         st.experimental_rerun()  # Force a rerun to navigate to the home page

            # except Exception as e:
            #     st.error(f"An error occurred while processing the file: {e}")
            
                # Use Streamlit's st.button with CSS styling applied
                if st.button("Proceed to Home Page", key="proceed_button"):
                    # Update session state instead of forcing a rerun
                    st.session_state["data_uploaded"] = True
                    st.session_state["current_page"] = "Home"

            except Exception as e:
                st.error(f"An error occurred while processing the file: {e}")

    # Middle Column: Vertical Line
    with divider:
        st.markdown(
            """
            <style>
            .vertical-line {
                border-left: 2px solid #ccc;
                height: 100vh;
                margin: 0 auto;
            }
            </style>
            <div class="vertical-line"></div>
            """, 
            unsafe_allow_html=True
        )

    # Right Column: Data Format Guidelines and SQL Query Explanation (2/3)
    with col2:
        st.markdown("<h1 style='color: #007bff;'>Data Format Guidelines for Market Basket Analysis</h1>", unsafe_allow_html=True)

        st.markdown("""
        Welcome to the **data upload page**. The dataset you upload will be used for performing a comprehensive **Market Basket Analysis**. 
        To ensure the analysis is accurate and insightful, please upload **transactional data** representing the purchases made by customers.

        The required dataset can be sourced from the **`oracle_data`** and **`oracle_data_product_name`** tables, which are available in the **`capstonetest`** database. 
        """)

        st.markdown("""
        The uploaded dataset should contain the following key columns:
        - **Order Number**: A unique identifier for each transaction.
        - **Item**: The specific item ordered.
        - **Quantity**: The quantity of the item ordered.
        - **Price**: The selling price of the item.
        - **Date**: The date when the order was placed.
        - **Category**: The category to which the item belongs.
        """)

        # The SQL Query Reveal Button
        if st.button("Need help with querying the data? Click here"):
            st.markdown("""
            ```sql
            SELECT 
                oracle_data.operating_unit_name,
                oracle_data.ecom_reference_order_number,
                oracle_data.ordered_item,
                oracle_data.ordered_quantity,
                oracle_data.unit_selling_price,
                oracle_data.unit_list_price,
                oracle_data.ordered_date,
                oracle_data.tax_code,
                oracle_data_product_name.product_id,
                oracle_data_product_name.product_name,
                oracle_data_product_name.product_category
            FROM 
                oracle_data
            JOIN 
                oracle_data_product_name 
            ON 
                oracle_data.ordered_item = oracle_data_product_name.product_id
            WHERE 
                oracle_data.ordered_date BETWEEN 'start_date' AND 'end_date';
            ```

            Please replace `'start_date'` and `'end_date'` with your desired date range to filter the data accordingly.
            """)


#-----------------------------------------------------------------------------------------------------------------

# Home Page Function
def home_page():
    # Main Title
    st.markdown(
        "<h1 style='font-size:40px; text-align: center; color:#007bff;'>AUB x Malia Market Basket Analysis Application</h1>",
        unsafe_allow_html=True
    )

    # Key Features Section
    st.markdown("""
    <div style="background-color:#f9f9f9; padding:20px; border-radius:10px; margin-bottom:20px;">
        <h2 style='color:#007bff;'>Key Features</h2>
        <p style="font-size:20px; line-height:1.6;">
            This application provides an interactive interface for performing market basket analysis using the <strong>Apriori</strong> and <strong>FP-Growth</strong> algorithms. 
            It helps you uncover hidden patterns in customer purchases, enabling better product placement, promotions, and inventory management.
        </p>
        <ul style="font-size:20px; line-height:1.6;">
            <li><strong>Data Exploration:</strong> Get an overview of your transaction data with comprehensive Exploratory Data Analysis (EDA).</li>
            <li><strong>Model Building:</strong> Generate association rules using Apriori and FP-Growth models.</li>
            <li><strong>Insights and Recommendations:</strong> Analyze the generated rules and gain actionable business insights.</li>
        </ul>
    </div>
    """, unsafe_allow_html=True)

    # How to Use Section
    st.markdown("""
    <div style="background-color:#f0f8ff; padding:20px; border-radius:10px; margin-bottom:20px;">
        <h2 style='color:#007bff;'>How to Use</h2>
        <p style="font-size:20px; line-height:1.6;">
            Follow these steps to utilize the application:
        </p>
        <ol style="font-size:20px; line-height:1.6;">
            <li><strong>Data Page:</strong> Explore and understand your transaction data.</li>
            <li><strong>Model Configuration:</strong> Set parameters and build your models.</li>
            <li><strong>Results and Insights:</strong> View and analyze the generated rules and insights.</li>
        </ol>
        <p style="font-size:20px;">
            We hope you find this application valuable for uncovering meaningful insights from your sales data. For further assistance, please refer to the documentation or contact support.
        </p>
    </div>
    """, unsafe_allow_html=True)

    # Resources Section
    st.markdown("""
    <div style="background-color:#f9f9f9; padding:20px; border-radius:10px; margin-bottom:20px;">
        <h2 style='color:#007bff;'>Additional Resources</h2>
        <p style="font-size:20px; line-height:1.6;">
            For more information on market basket analysis, visit:
        </p>
        <ul style="font-size:20px; line-height:1.6;">
            <li><a href="https://en.wikipedia.org/wiki/Market_basket_analysis" target="_blank">Market Basket Analysis - Wikipedia</a></li>
            <li><a href="https://en.wikipedia.org/wiki/Association_rule_learning" target="_blank">Association Rule Learning - Wikipedia</a></li>
        </ul>
    </div>
    """, unsafe_allow_html=True)

    # Footer Section
    st.markdown("""
    <div style="font-size:16px; color:gray; text-align:right; margin-top:30px;">
        <p>To Malia Group, By AUB Students</p>
    </div>
    """, unsafe_allow_html=True)



#----------------------------------------------------------------------------------------------------------------------

def data_overview_page():
    # Main Title
    st.markdown(
        "<h1 style='font-size:40px; text-align: center; color:#007bff;'>Data Overview</h1>",
        unsafe_allow_html=True
    )


    # Display introduction and dataset description with larger text
    st.markdown("""
    <div style="font-size:24px;">
        This page provides a detailed overview of the e-commerce order transactions data used in the Market Basket Analysis application. 
        It includes information about orders, products, pricing, and quantities that are essential for uncovering patterns in customer purchasing behavior.
        Below are some key statistics about the dataset:
    </div>
    """, unsafe_allow_html=True)


    # Calculate key statistics
    num_rows = st.session_state["data"].shape[0]
    num_columns = st.session_state["data"].shape[1]
    num_orders = st.session_state["data"]["ecom_reference_order_number"].nunique()
    num_unique_items = st.session_state["data"]["ordered_item"].nunique()
    avg_basket_value = st.session_state["data"].groupby("ecom_reference_order_number")["total_sales_with_discount"].sum().mean()
    avg_items_per_transaction = st.session_state["data"]["ordered_quantity"].mean()
    avg_items_per_order = st.session_state["data"].groupby("ecom_reference_order_number")["ordered_quantity"].sum().mean()

    # Additional metrics
    total_revenue = st.session_state["data"]["total_sales_with_discount"].sum()
    top_product_category = st.session_state["data_expanded"]["product_category"].value_counts().idxmax()
    avg_discount = st.session_state["data"]["discount_percentage"].mean()

    # First row of tiles
    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown(f"""
        <div style="text-align: center; padding: 20px; margin: 10px; background-color: #f0f8ff; border-radius: 10px; box-shadow: 0px 4px 10px rgba(0,0,0,0.1);">
            <h1 style="font-size: 48px; color: #007bff;">{num_rows}</h1>
            <p style="font-size: 18px;">Total Rows</p>
        </div>
        """, unsafe_allow_html=True)

    with col2:
        st.markdown(f"""
        <div style="text-align: center; padding: 20px; margin: 10px; background-color: #f9f9f9; border-radius: 10px; box-shadow: 0px 4px 10px rgba(0,0,0,0.1);">
            <h1 style="font-size: 48px; color: #007bff;">{num_columns}</h1>
            <p style="font-size: 18px;">Total Columns</p>
        </div>
        """, unsafe_allow_html=True)

    with col3:
        st.markdown(f"""
        <div style="text-align: center; padding: 20px; margin: 10px; background-color: #f0f8ff; border-radius: 10px; box-shadow: 0px 4px 10px rgba(0,0,0,0.1);">
            <h1 style="font-size: 48px; color: #007bff;">{num_orders}</h1>
            <p style="font-size: 18px;">Total Orders</p>
        </div>
        """, unsafe_allow_html=True)

    # Second row of tiles
    col4, col5, col6 = st.columns(3)

    with col4:
        st.markdown(f"""
        <div style="text-align: center; padding: 20px; margin: 10px; background-color: #f9f9f9; border-radius: 10px; box-shadow: 0px 4px 10px rgba(0,0,0,0.1);">
            <h1 style="font-size: 48px; color: #007bff;">{num_unique_items}</h1>
            <p style="font-size: 18px;">Unique Items</p>
        </div>
        """, unsafe_allow_html=True)

    with col5:
        st.markdown(f"""
        <div style="text-align: center; padding: 20px; margin: 10px; background-color: #f0f8ff; border-radius: 10px; box-shadow: 0px 4px 10px rgba(0,0,0,0.1);">
            <h1 style="font-size: 48px; color: #007bff;">${total_revenue:,.2f}</h1>
            <p style="font-size: 18px;">Total Revenue</p>
        </div>
        """, unsafe_allow_html=True)

    with col6:
        st.markdown(f"""
        <div style="text-align: center; padding: 20px; margin: 10px; background-color: #f9f9f9; border-radius: 10px; box-shadow: 0px 4px 10px rgba(0,0,0,0.1);">
            <h1 style="font-size: 48px; color: #007bff;">${avg_basket_value:.2f}</h1>
            <p style="font-size: 18px;">Avg Basket Value</p>
        </div>
        """, unsafe_allow_html=True)

    # Third row of tiles for additional metrics
    col7, col8, col9 = st.columns(3)

    with col7:
        st.markdown(f"""
        <div style="text-align: center; padding: 20px; margin: 10px; background-color: #f0f8ff; border-radius: 10px; box-shadow: 0px 4px 10px rgba(0,0,0,0.1);">
            <h1 style="font-size: 48px; color: #007bff;">{avg_discount:.2f}%</h1>
            <p style="font-size: 18px;">Avg Discount</p>
        </div>
        """, unsafe_allow_html=True)

    with col8:
        st.markdown(f"""
        <div style="text-align: center; padding: 20px; margin: 10px; background-color: #f9f9f9; border-radius: 10px; box-shadow: 0px 4px 10px rgba(0,0,0,0.1);">
            <h1 style="font-size: 48px; color: #007bff;">{top_product_category}</h1>
            <p style="font-size: 18px;">Top Product Category</p>
        </div>
        """, unsafe_allow_html=True)

    with col9:
        st.markdown(f"""
        <div style="text-align: center; padding: 20px; margin: 10px; background-color: #f0f8ff; border-radius: 10px; box-shadow: 0px 4px 10px rgba(0,0,0,0.1);">
            <h1 style="font-size: 48px; color: #007bff;">{avg_items_per_transaction:.2f}</h1>
            <p style="font-size: 18px;">Avg Items per Transaction</p>
        </div>
        """, unsafe_allow_html=True)



#----------------------------------------------------------------------------------------------------------------------


import plotly.express as px

def eda_page():
#    st.title("Exploratory Data Analysis (EDA)")

    # Create a tab layout for the EDA categories
    tabs = st.tabs(["Sales Analysis", "Order Analysis", "Pricing and Discount Analysis", "Supply Chain Analysis", "Product Positioning Analysis"])

    # Sales Analysis Tab
    with tabs[0]:
        st.markdown("""
        <div style="background-color: #f0f8ff; padding: 20px; border-radius: 10px; box-shadow: 0px 4px 10px rgba(0,0,0,0.1);">
            <h2 style="text-align: center;">Sales Analysis Dashboard</h2>
            <p style="font-size:18px; line-height:1.6;">
            The Sales Analysis Dashboard provides a comprehensive overview of sales performance across various metrics, 
            including revenue distribution, customer purchasing behaviors, and monthly sales trends. By analyzing the visualizations, you can identify the most 
            significant product categories, understand the distribution of order quantities, and explore the monetary distribution of transactions.
            </p>
        </div>
        """, unsafe_allow_html=True)

        st.markdown("<br><br>", unsafe_allow_html=True)  # Add white space between title and visualizations

        # First row: Revenue Contribution by Product Category and Distribution of Order Quantities
        col1, col2 = st.columns(2)
        
        with col1:
            # Revenue Contribution by Product Category (sorted by total sales descending)
            category_sales = st.session_state["data_expanded"].groupby('product_category')['total_sales_with_discount'].sum().reset_index().sort_values(by='total_sales_with_discount', ascending=False)
            
            fig = px.bar(
                category_sales,
                x='product_category',
                y='total_sales_with_discount',
                labels={'product_category': 'Product Category', 'total_sales_with_discount': 'Total Sales'},
                title="Revenue Contribution by Product Category"
            )
            fig.update_layout(xaxis_tickangle=-45, title_x=0.3, title_font_size=20)
            fig.update_xaxes(showgrid=False)
            fig.update_yaxes(showgrid=False)
            st.plotly_chart(fig)

        with col2:
            # Distribution of Order Quantities
            fig = px.histogram(
                st.session_state["data"],
                x='ordered_quantity',
                nbins=30,
                title="Distribution of Order Quantities",
                labels={'ordered_quantity': 'Order Quantity'},
            )
            fig.update_layout(title_x=0.35, title_font_size=20)
            fig.update_xaxes(showgrid=False)
            fig.update_yaxes(showgrid=False)
            st.plotly_chart(fig)

        st.markdown("<br><br>", unsafe_allow_html=True)  # Add white space between rows

        # Second row: Monetary Distribution and Monthly Sales Trend (Filtered for Jan, Feb, and March)
        col3, col4 = st.columns(2)

        with col3:
            # Monetary Distribution
            monetary = st.session_state["data"].groupby('ecom_reference_order_number')['total_sales_with_discount'].sum().reset_index()
            
            fig = px.histogram(
                monetary,
                x='total_sales_with_discount',
                nbins=30,
                title="Monetary Distribution",
                labels={'total_sales_with_discount': 'Total Spend'},
            )
            fig.update_layout(title_x=0.4, title_font_size=20)
            fig.update_xaxes(showgrid=False)
            fig.update_yaxes(showgrid=False)
            st.plotly_chart(fig)

        with col4:
            # Monthly Sales Trend as Line Chart
            df = st.session_state["data"]
            df['ordered_date'] = pd.to_datetime(df['ordered_date'], errors='coerce')
            df['year_month'] = df['ordered_date'].dt.to_period('M').astype(str)  # Convert Period to string

            monthly_sales = df.groupby('year_month')['total_sales_with_discount'].sum().reset_index()

            # Create a line chart instead of a bar chart
            fig = px.line(
                monthly_sales,
                x='year_month',
                y='total_sales_with_discount',
                labels={'year_month': 'Month', 'total_sales_with_discount': 'Total Sales with Discount'},
                title="Monthly Sales Trend"
            )

            fig.update_layout(
                xaxis_tickangle=-45,
                title_x=0.4,
                title_font_size=20
            )

            # Plot the line chart
            st.plotly_chart(fig)
            

        st.markdown("<br><br>", unsafe_allow_html=True)  # Add white space between rows

        # Third row: Top 10 Best-Selling Products (full width, larger height)
        # Top 10 Best-Selling Products (last plot)
        top_products = st.session_state["data"][st.session_state["data"]['product_name'] != 'Product Not Found']\
                        .groupby('product_name')['ordered_quantity']\
                        .sum().sort_values(ascending=False).head(10)
        
        fig = px.bar(
            top_products,
            x=top_products.index,
            y=top_products.values,
            labels={'x': 'Product Name', 'y': 'Total Quantity Sold'},
            title="Top 10 Best-Selling Products"
        )
        fig.update_layout(xaxis_tickangle=-45, title_x=0.45, title_font_size=20, height=700)
        fig.update_xaxes(showgrid=False)
        fig.update_yaxes(showgrid=False)
        st.plotly_chart(fig, use_container_width=True)  # Full width for this plot

    # Order Analysis Tab
    with tabs[1]:
        st.markdown("""
        <div style='background-color: #f0f8ff; padding: 20px; border-radius: 10px; box-shadow: 0px 4px 10px rgba(0,0,0,0.1);'>
            <h2 style="text-align: center;">Order Analysis Dashboard</h2>
            <p style="font-size:18px; line-height:1.6;">
            The Order Analysis Dashboard provides detailed insights into order frequency, quantity distributions, and customer behavior patterns. 
            The visualizations below will help identify recency distributions, statistics about order quantities, and how many items are typically included in each order.
            </p>
        </div>
        """, unsafe_allow_html=True)

        st.markdown("<br><br>", unsafe_allow_html=True)  # Add white space between title and visualizations

        # First row: Recency Distribution and Order Quantity Statistics
        col1, col2 = st.columns(2)
        
        with col1:
            # Recency Distribution
            df = st.session_state["data"]
            df['ordered_date'] = pd.to_datetime(df['ordered_date'], errors='coerce')
            df['recency'] = (df['ordered_date'].max() - df['ordered_date']).dt.days
            rfm = pd.DataFrame({'recency': df.groupby('ecom_reference_order_number')['recency'].min()})
            
            fig = px.histogram(
                rfm,
                x='recency',
                nbins=30,
                title="Recency Distribution",
                labels={'recency': 'Days Since Last Purchase'},
            )
            fig.update_layout(title_x=0.35, title_font_size=20)
            fig.update_xaxes(showgrid=False)
            fig.update_yaxes(showgrid=False)
            st.plotly_chart(fig)

        with col2:
            # Order Quantity Statistics
            order_quantity_stats = st.session_state["data"].groupby('ecom_reference_order_number')['ordered_quantity'].sum()
            summary_stats = order_quantity_stats.describe()

            stats = {
                'Statistic': ['Average', 'Minimum', 'Maximum', 'Q1 (25th percentile)', 'Median (50th percentile)', 'Q3 (75th percentile)'],
                'Value': [
                    summary_stats['mean'],
                    summary_stats['min'],
                    summary_stats['max'],
                    summary_stats['25%'],
                    summary_stats['50%'],
                    summary_stats['75%']
                ]
            }
            stats_df = pd.DataFrame(stats)

            st.markdown("<h3 style='text-align: center;'>Order Quantity Statistics</h3>", unsafe_allow_html=True)
            st.table(stats_df)

        st.markdown("<br><br>", unsafe_allow_html=True)  # Add white space between rows

        # Second row: Distribution of Items per Order
        col3, _ = st.columns([2, 1])  # First column with a larger width
        
        with col3:
            # Distribution of Items per Order
            order_quantity_stats = st.session_state["data"].groupby('ecom_reference_order_number')['ordered_quantity'].sum()

            fig = px.histogram(
                order_quantity_stats,
                x=order_quantity_stats,
                nbins=30,
                title="Distribution of Items per Order",
                labels={'x': 'Number of Items per Order'},
            )
            fig.update_layout(title_x=0.35, title_font_size=20)
            fig.update_xaxes(showgrid=False)
            fig.update_yaxes(showgrid=False)
            st.plotly_chart(fig)

    # Pricing and Discount Analysis Tab
    with tabs[2]:
        st.markdown("""
        <div style='background-color: #f0f8ff; padding: 20px; border-radius: 10px; box-shadow: 0px 4px 10px rgba(0,0,0,0.1);'>
            <h2 style="text-align: center;">Pricing and Discount Analysis Dashboard</h2>
            <p style="font-size:18px; line-height:1.6;">
            This dashboard focuses on analyzing the pricing strategies and discount applications across different products and categories. 
            Explore the distribution of selling and list prices, the impact of discounts on sales, and how discounts vary across product categories and popularity.
            </p>
        </div>
        """, unsafe_allow_html=True)

        st.markdown("<br><br>", unsafe_allow_html=True)  # Add white space between title and visualizations

        # First row: Distribution of Selling Prices and Distribution of Discount Percentages
        col1, col2 = st.columns(2)
        
        with col1:
            # Distribution of Selling Prices and List Prices
            fig = px.histogram(
                st.session_state["data"],
                x=["unit_selling_price", "unit_list_price"],
                nbins=20,
                title="Distribution of Selling Prices and List Prices",
                labels={'x': 'Price'},
            )
            fig.update_layout(title_x=0.35, title_font_size=20, barmode='overlay')
            fig.update_xaxes(showgrid=False)
            fig.update_yaxes(showgrid=False)
            st.plotly_chart(fig)

        with col2:
            # Distribution of Discount Percentages
            st.session_state["data"]['discount_percentage'] = (st.session_state["data"]['unit_list_price'] - st.session_state["data"]['unit_selling_price']) / st.session_state["data"]['unit_list_price'] * 100
            
            fig = px.histogram(
                st.session_state["data"],
                x='discount_percentage',
                nbins=20,
                title="Distribution of Discount Percentages",
                labels={'discount_percentage': 'Discount Percentage'},
            )
            fig.update_layout(title_x=0.35, title_font_size=20)
            fig.update_xaxes(showgrid=False)
            fig.update_yaxes(showgrid=False)
            st.plotly_chart(fig)

        st.markdown("<br><br>", unsafe_allow_html=True)  # Add white space between rows

        # Second row: Discount Percentage by Category and Scatter Plot of Discount Percentage vs. Ordered Quantity
        col3, col4 = st.columns(2)
        
        with col3:
            # Discount Percentage by Category
            fig = px.box(
                st.session_state["data_expanded"],
                x='product_category',
                y='discount_percentage',
                title="Discount Percentage by Category",
                labels={'product_category': 'Product Category', 'discount_percentage': 'Discount Percentage'},
            )
            fig.update_layout(title_x=0.35, title_font_size=20)
            fig.update_xaxes(showgrid=False, tickangle=-90)
            fig.update_yaxes(showgrid=False)
            st.plotly_chart(fig)

        with col4:
            # Scatter Plot of Discount Percentage vs. Ordered Quantity
            fig = px.scatter(
                st.session_state["data"],
                x='discount_percentage',
                y='ordered_quantity',
                title="Scatter Plot of Discount Percentage vs. Ordered Quantity",
                labels={'discount_percentage': 'Discount Percentage', 'ordered_quantity': 'Ordered Quantity'},
            )
            fig.update_layout(title_x=0.20, title_font_size=20)
            fig.update_xaxes(showgrid=False)
            fig.update_yaxes(showgrid=False)
            st.plotly_chart(fig)

        st.markdown("<br><br>", unsafe_allow_html=True)  # Add white space between rows

        # Third row: Comparison of Average Discounts for Popular and Less Popular Items and Box Plot of Unit Selling Prices by Top Performing Product Categories
        col5, col6 = st.columns(2)
        
        with col5:
            # Comparison of Average Discounts for Popular and Less Popular Items
            item_popularity = st.session_state["data"].groupby('product_name')['ordered_quantity'].sum()
            median_popularity = item_popularity.median()
            st.session_state["data"]['popularity'] = st.session_state["data"]['product_name'].map(lambda x: 'Popular' if item_popularity[x] > median_popularity else 'Less Popular')
            average_discounts = st.session_state["data"].groupby('product_name')['discount_percentage'].mean()
            st.session_state["data"]['average_discount'] = st.session_state["data"]['product_name'].map(average_discounts)

            comparison_df = st.session_state["data"][['product_name', 'popularity', 'average_discount']].drop_duplicates()
            
            fig = px.histogram(
                comparison_df,
                x='average_discount',
                color='popularity',
                title="Comparison of Average Discounts for Popular and Less Popular Items",
                labels={'average_discount': 'Average Discount Percentage'},
                nbins=20,
            )
            fig.update_layout(title_x=0.20, title_font_size=20)
            fig.update_xaxes(showgrid=False)
            fig.update_yaxes(showgrid=False)
            st.plotly_chart(fig)

        with col6:
            # Box Plot of Unit Selling Prices by Top Performing Product Categories
            top_categories = st.session_state["data_expanded"].groupby('product_category')['total_sales_with_discount'].sum().sort_values(ascending=False).head(10).index.tolist()
            top_categories_df = st.session_state["data_expanded"][st.session_state["data_expanded"]['product_category'].isin(top_categories)]
            
            fig = px.box(
                top_categories_df,
                x='product_category',
                y='unit_selling_price',
                title="Box Plot of Unit Selling Prices by Top Performing Product Categories",
                labels={'product_category': 'Product Category', 'unit_selling_price': 'Unit Selling Price'},
            )
            fig.update_layout(title_x=0.20, title_font_size=20)
            fig.update_xaxes(tickangle=-90, showgrid=False)
            fig.update_yaxes(showgrid=False)
            st.plotly_chart(fig)
            
    with tabs[3]:
        st.markdown("""
        <div style='background-color: #f0f8ff; padding: 20px; border-radius: 10px; box-shadow: 0px 4px 10px rgba(0,0,0,0.1);'>
            <h2 style="text-align: center;">Supply Chain Analysis Dashboard</h2>
            <p style="font-size:18px; line-height:1.6;">
            The Supply Chain Analysis Dashboard provides insights into the distribution activities and supply chain efficiency. 
            Explore the top-performing distributors, shipment quantities, and their relative contributions to the supply chain.
            </p>
        </div>
        """, unsafe_allow_html=True)

        st.markdown("<br><br>", unsafe_allow_html=True)  # Add white space between title and visualizations

        # Bar Chart of Top Distributing Companies by Quantity Shipped
        top_distributors = st.session_state["data"].groupby('operating_unit_name')['ordered_quantity'].sum().sort_values(ascending=False).head(10)

        # Create a bar chart using Plotly
        fig = px.bar(
            top_distributors.reset_index(),
            x='operating_unit_name',
            y='ordered_quantity',
            labels={'operating_unit_name': 'Distributing Company', 'ordered_quantity': 'Quantity Shipped'},
            title='Top Distributing Companies by Quantity Shipped'
        )

        # Customize the layout of the chart
        fig.update_layout(
            title_x=0.2,  # Center the title
            title_font_size=20,
            xaxis_tickangle=-45,  # Rotate x-axis labels for better readability
            xaxis_title_font=dict(size=15),
            yaxis_title_font=dict(size=15),
            showlegend=False,  # Hide legend
            plot_bgcolor='rgba(0,0,0,0)',  # Transparent background
            paper_bgcolor='rgba(0,0,0,0)',  # Transparent paper background
            margin=dict(l=100, r=100, t=50, b=50)  # Add margins to center the plot
        )

        # Center the plot on the page
        st.markdown("<div style='text-align: center;'>", unsafe_allow_html=True)
        st.plotly_chart(fig, use_container_width=False)
        st.markdown("</div>", unsafe_allow_html=True)

    with tabs[4]:
        st.markdown("""
        <div style='background-color: #f0f8ff; padding: 20px; border-radius: 10px; box-shadow: 0px 4px 10px rgba(0,0,0,0.1);'>
            <h2 style="text-align: center;">Product Positioning Dashboard</h2>
            <p style="font-size:18px; line-height:1.6;">
            The Product Positioning Dashboard provides insights into the diversity and frequency of items purchased in transactions. 
            It explores customer purchasing behaviors and highlights niche products that may have unique market opportunities.
            </p>
        </div>
        """, unsafe_allow_html=True)

        st.markdown("<br><br>", unsafe_allow_html=True)  # Add white space between title and visualizations

        # First row: Distribution of the Number of Items per Transaction and Distribution of Item Frequencies
        col1, col2 = st.columns(2)

        with col1:
            # Distribution of the Number of Items per Transaction
            basket = (st.session_state["data"]
                        .groupby(['ecom_reference_order_number', 'product_name'])['product_name']
                        .count().unstack().reset_index().fillna(0)
                        .set_index('ecom_reference_order_number'))
            basket_sets = basket.applymap(lambda x: 1 if x > 0 else 0)
            unique_items_per_transaction = basket_sets.sum(axis=1)

            fig = px.histogram(
                unique_items_per_transaction, 
                nbins=30,
                title="Distribution of the Number of Items per Transaction",
                labels={'value': 'Number of Items', 'count': 'Frequency'}
            )
            fig.update_layout(
                title_x=0.2, 
                title_font_size=20, 
                xaxis=dict(showgrid=False), 
                yaxis=dict(showgrid=False),
                plot_bgcolor='rgba(0,0,0,0)'
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Distribution of Item Frequencies
            item_frequencies = basket_sets.sum(axis=0)

            fig = px.histogram(
                item_frequencies, 
                nbins=30,
                title="Distribution of Item Frequencies",
                labels={'value': 'Frequency of Items', 'count': 'Number of Items'}
            )
            fig.update_layout(
                title_x=0.2, 
                title_font_size=20, 
                xaxis=dict(showgrid=False), 
                yaxis=dict(showgrid=False),
                plot_bgcolor='rgba(0,0,0,0)'
            )
            st.plotly_chart(fig, use_container_width=True)

        st.markdown("<br><br>", unsafe_allow_html=True)  # Add white space between rows

        # Second row: Transaction Diversity Statistics and Item Frequencies Statistics
        col3, col4 = st.columns(2)

        with col3:
            # Transaction Diversity Statistics
            stats = unique_items_per_transaction.describe()
            stats_df = pd.DataFrame(stats, columns=['Value']).reset_index()
            stats_df.columns = ['Statistic', 'Value']
            st.markdown("<h3 style='text-align: center;'>Transaction Diversity Statistics</h3>", unsafe_allow_html=True)
            st.table(stats_df)

        with col4:
            # Item Frequencies Statistics
            stats = item_frequencies.describe()
            stats_df = pd.DataFrame(stats, columns=['Value']).reset_index()
            stats_df.columns = ['Statistic', 'Value']
            st.markdown("<h3 style='text-align: center;'>Item Frequencies Statistics</h3>", unsafe_allow_html=True)
            st.table(stats_df)

        st.markdown("<br><br>", unsafe_allow_html=True)  # Add white space between rows

        # Third row: Considered as Niche Products
        st.markdown("<h3 style='text-align: center;'>Products Considered Niche</h3>", unsafe_allow_html=True)

        sales_volume = st.session_state["data"].groupby('product_name')['ordered_quantity'].sum().reset_index()
        niche_products = sales_volume[sales_volume['ordered_quantity'] < sales_volume['ordered_quantity'].quantile(0.25)]
        purchase_frequency = st.session_state["data"].groupby('product_name')['ecom_reference_order_number'].nunique().reset_index()
        niche_purchase_frequency = purchase_frequency[purchase_frequency['ecom_reference_order_number'] < purchase_frequency['ecom_reference_order_number'].quantile(0.25)]

        combined_niche_products = set(niche_products['product_name']).intersection(
            set(niche_purchase_frequency['product_name'])
        )

        combined_niche_products_df = pd.DataFrame({
            'Product Name': list(combined_niche_products)
        })

        st.table(combined_niche_products_df)


#----------------------------------------------------------------------------------------------------------------------

def run_model(df, model_choice, min_support, min_confidence, min_lift):
    # Preprocess the data into a basket format
    basket = (df
              .groupby(['ecom_reference_order_number', 'product_name'])['product_name']
              .count().unstack().reset_index().fillna(0)
              .set_index('ecom_reference_order_number'))
    basket_sets = basket.applymap(lambda x: 1 if x > 0 else 0)

    # Choose between Apriori or FP-Growth
    if model_choice == "Apriori":
        frequent_itemsets = apriori(basket_sets, min_support=min_support, use_colnames=True)
    elif model_choice == "FP-Growth":
        frequent_itemsets = fpgrowth(basket_sets, min_support=min_support, use_colnames=True)
    
    # Check if frequent itemsets are found
    if frequent_itemsets.empty:
        return None

    # Generate association rules based on confidence and lift thresholds
    rules = association_rules(frequent_itemsets, metric="confidence", min_threshold=min_confidence)
    filtered_rules = rules[rules['lift'] >= min_lift]
    
    # Format the antecedents and consequents as strings for better readability
    filtered_rules['antecedents'] = filtered_rules['antecedents'].apply(lambda x: ', '.join(list(x)))
    filtered_rules['consequents'] = filtered_rules['consequents'].apply(lambda x: ', '.join(list(x)))
    
    # Reset the index and return the rules
    filtered_rules.reset_index(drop=True, inplace=True)
    return filtered_rules

# Define the model configuration page function correctly
def model_config_page():

    st.markdown(
        "<h1 style='font-size:40px; text-align: center; color:#007bff;'>Market Basket Analysis</h1>",
        unsafe_allow_html=True
    )

    
    # Introduction and explanation in a container
    st.markdown("""
    <div style='background-color: #f0f8ff; padding: 20px; border-radius: 10px; box-shadow: 0px 4px 10px rgba(0,0,0,0.1); margin-bottom: 30px;'>
        <p style="font-size:18px; line-height:1.6;">
        Market Basket Analysis is a technique used to identify associations or relationships between products. 
        It uses two primary algorithms: <strong>Apriori</strong> and <strong>FP-Growth</strong>.
        </p>
        <h3>Key Metrics:</h3>
        <ul>
            <li><strong>Support:</strong> The proportion of transactions that contain the itemset.</li>
            <li><strong>Confidence:</strong> A measure of the reliability of the rule. It is calculated as the ratio of transactions containing the antecedent.</li>
            <li><strong>Lift:</strong> A value greater than 1 indicates a strong association between the antecedent and the consequent.</li>
        </ul>
    </div>
    """, unsafe_allow_html=True)

    # Model selection with icons (optional)
    st.subheader("Select Model and Set Parameters")
    model_choice = st.radio(
        "Choose the algorithm:", 
        ("Apriori", "FP-Growth"),
        format_func=lambda x: f"{x} {'🔗' if x == 'Apriori' else '📈'}"  # Adding icons
    )

    # Expandable section for setting parameters
    with st.expander("Set Model Parameters", expanded=False):
        st.markdown("### Common Parameters")
        min_support = st.slider("Minimum Support:", min_value=0.01, max_value=1.0, value=0.02, step=0.01)
        min_confidence = st.slider("Minimum Confidence:", min_value=0.1, max_value=1.0, value=0.7, step=0.1)
        min_lift = st.slider("Minimum Lift:", min_value=1.0, max_value=20.0, value=10.0, step=0.5)

        # Show the selected parameters in a summary box
        st.markdown(f"""
        <div style='background-color: #eaf2f8; padding: 10px; border-radius: 8px;'>
            <h4 style="text-align: center;">Selected Parameters</h4>
            <p><strong>Model:</strong> {model_choice}</p>
            <p><strong>Minimum Support:</strong> {min_support}</p>
            <p><strong>Minimum Confidence:</strong> {min_confidence}</p>
            <p><strong>Minimum Lift:</strong> {min_lift}</p>
        </div>
        """, unsafe_allow_html=True)

    # Run Model Button with real-time feedback
    st.markdown("<br>", unsafe_allow_html=True)
    if st.button("Run Model", help="Run the selected algorithm with the specified parameters"):
        with st.spinner("Running the model..."):
            result = run_model(st.session_state['data'], model_choice, min_support, min_confidence, min_lift)
            if result is None or result.empty:
                st.error("No rules found with the given combination of parameters. Please adjust the thresholds.")
            else:
                st.success(f"Model run successfully! Number of rules: {len(result)}")
                st.write(result)
                st.session_state['results'] = result  # Store results in session state

    # Footer
    st.markdown("""
    <div style="position: fixed; bottom: 10px; right: 10px; font-size:16px; color:gray;">
        To Malia Group, By AUB Students
    </div>
    """, unsafe_allow_html=True)

#-----------------------------------------------------------------------------------------------------------------------


def model_insights_page():
    # Header section
    st.markdown("""
    <div style='background-color: #f0f8ff; padding: 20px; border-radius: 10px; box-shadow: 0px 4px 10px rgba(0,0,0,0.1);'>
        <h2 style="text-align: center;">Results and Visualizations</h2>
        <p style="font-size:18px; line-height:1.6; text-align: center;">
            Explore the results of the Market Basket Analysis. Visualize key association rules and filter them by antecedents.
            Additionally, examine the distribution of support, confidence, and lift for a deeper understanding of the generated rules.
        </p>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)  # Add white space

    # Load the generated rules
    if 'results' in st.session_state:
        results = st.session_state['results']

        # First Row: Scatter Plot and Explanation
        col1, col2 = st.columns([3, 2])  # Wider column for plot, narrower for description

        with col1:
            st.markdown("<h3 style='text-align: center;'>Scatter Plot of Support and Confidence</h3>", unsafe_allow_html=True)
            def plot_support_confidence(results):
                fig = px.scatter(
                    results, 
                    x='support', y='confidence', 
                    size='lift', color='lift',
                    title="Support vs Confidence",
                    labels={'support': 'Support', 'confidence': 'Confidence'},
                    hover_data=['antecedents', 'consequents'],
                    color_continuous_scale='Viridis'
                )
                fig.update_traces(marker=dict(line=dict(width=2, color='DarkSlateGrey')))
                fig.update_layout(title_x=0.5, plot_bgcolor='rgba(0,0,0,0)')
                st.plotly_chart(fig, use_container_width=True)
            
            plot_support_confidence(results)
        
        with col2:
            st.markdown("""
            <div style='font-size:18px;'>
                <br>The scatter plot visualizes the association rules with the x-axis representing the support, and the y-axis representing the confidence of each rule.
                The size and color of the points represent the lift of the rule.
                <br><br>Use this plot to identify significant association rules that can help in better decision-making and marketing strategies.
            </div>
            """, unsafe_allow_html=True)

        st.markdown("<hr>", unsafe_allow_html=True)  # Separator

        # Second Row: Support, Confidence, and Lift Distributions (3 plots in one row)
        st.markdown("<h3 style='text-align: center;'>Distributions of Support, Confidence, and Lift</h3>", unsafe_allow_html=True)
        col3, col4, col5 = st.columns(3)

        with col3:
            st.markdown("<h4 style='text-align: center;'>Support Distribution</h4>", unsafe_allow_html=True)
            fig, ax = plt.subplots(figsize=(6, 4))
            sns.histplot(results['support'], bins=20, kde=True, ax=ax)
            ax.set_xlabel('Support')
            ax.set_ylabel('Frequency')
            ax.grid(False)
            st.pyplot(fig)

        with col4:
            st.markdown("<h4 style='text-align: center;'>Confidence Distribution</h4>", unsafe_allow_html=True)
            fig, ax = plt.subplots(figsize=(6, 4))
            sns.histplot(results['confidence'], bins=20, kde=True, ax=ax)
            ax.set_xlabel('Confidence')
            ax.set_ylabel('Frequency')
            ax.grid(False)
            st.pyplot(fig)

        with col5:
            st.markdown("<h4 style='text-align: center;'>Lift Distribution</h4>", unsafe_allow_html=True)
            fig, ax = plt.subplots(figsize=(6, 4))
            sns.histplot(results['lift'], bins=20, kde=True, ax=ax)
            ax.set_xlabel('Lift')
            ax.set_ylabel('Frequency')
            ax.grid(False)
            st.pyplot(fig)


 #       st.markdown("<br><br>", unsafe_allow_html=True)  # White space before the filtered rules section
        st.markdown("<hr>", unsafe_allow_html=True)  # Separator
        
        
        # Filter by Antecedents
        st.subheader("Filter Rules by Antecedents")
        all_antecedents = sorted(set(results['antecedents']))
        selected_antecedents = st.multiselect("Select Antecedents:", all_antecedents)

        # Filter the results based on antecedents
        if selected_antecedents:
            filtered_results = results[results['antecedents'].isin(selected_antecedents)]
        else:
            filtered_results = results

        # Display filtered rules using an accordion-style layout
        st.subheader("Association Rules")
        if filtered_results.empty:
            st.write("No rules found with the selected antecedents.")
        else:
            for index, row in filtered_results.iterrows():
                with st.expander(f"Rule {index + 1}: {row['antecedents']} -> {row['consequents']}"):
                    st.markdown(f"**Support:** {row['support']:.3f}")
                    st.markdown(f"**Confidence:** {row['confidence']:.3f}")
                    st.markdown(f"**Lift:** {row['lift']:.3f}")
                    st.markdown(f"**Leverage:** {row.get('leverage', 'N/A'):.3f}")
                    st.markdown(f"**Conviction:** {row.get('conviction', 'N/A'):.3f}")
                    st.markdown(f"**Zhang's Metric:** {row.get('zhangs_metric', 'N/A'):.3f}")
        
    else:
        st.write("Please run the model first to generate results.")
    
    st.markdown("<br><br>", unsafe_allow_html=True)  # White space

    # Footer
    st.markdown("""
    <div style="position: fixed; bottom: 10px; right: 10px; font-size:16px; color:gray;">
        To Malia Group, By AUB Students
    </div>
    """, unsafe_allow_html=True)



#-------------------------------------------------------------------------------------------------------------------

def render_page():
    # Only show the navigation bar after login and data upload
    if st.session_state.get("logged_in") and st.session_state.get("data_uploaded"):
        # Define the navigation options for the main app
        navigation_options = ["Home", "Data Overview", "EDA", "Model Configuration", "Model Insights"]

        # Ensure the current page is valid (must be one of the navigation options)
        if st.session_state["current_page"] not in navigation_options:
            st.session_state["current_page"] = "Home"  # Fallback to Home if invalid page

        # Custom CSS for the navigation bar
        st.markdown(
            """
            <style>
            .nav-container {
                display: flex;
                justify-content: space-evenly;
                background-color: #f8f9fa;
                padding: 10px;
                border-radius: 10px;
                margin-bottom: 20px;
                border: 1px solid #ddd;
            }
            .nav-button {
                width: 100%;
                background-color: #007bff;
                color: white;
                font-weight: bold;
                padding: 15px;
                border-radius: 0;
                text-align: center;
                cursor: pointer;
                text-decoration: none;
                transition: background-color 0.3s ease;
                border: none;
            }
            .nav-button:hover {
                background-color: #0056b3;
            }
            .nav-button.active {
                background-color: #0056b3;
            }
            .block-container {
                padding-top: 0px;
                padding-bottom: 0px;
            }
            </style>
            """,
            unsafe_allow_html=True
        )

        # Custom navigation bar
        st.markdown('<div class="nav-container">', unsafe_allow_html=True)
        col1, col2, col3, col4, col5 = st.columns([1, 1, 1, 1, 1])

        # Navigation without rerun
        with col1:
            if st.button("Home", key="home_button"):
                st.session_state["current_page"] = "Home"

        with col2:
            if st.button("Data Overview", key="overview_button"):
                st.session_state["current_page"] = "Data Overview"

        with col3:
            if st.button("EDA", key="eda_button"):
                st.session_state["current_page"] = "EDA"

        with col4:
            if st.button("Model Configuration", key="config_button"):
                st.session_state["current_page"] = "Model Configuration"

        with col5:
            if st.button("Model Insights", key="insights_button"):
                st.session_state["current_page"] = "Model Insights"

        st.markdown('</div>', unsafe_allow_html=True)

        # Route to the correct page based on session state
        if st.session_state["current_page"] == "Home":
            home_page()
        elif st.session_state["current_page"] == "Data Overview":
            data_overview_page()
        elif st.session_state["current_page"] == "EDA":
            eda_page()
        elif st.session_state["current_page"] == "Model Configuration":
            model_config_page()
        elif st.session_state["current_page"] == "Model Insights":
            model_insights_page()

    else:
        # If not logged in, show login page
        if not st.session_state.get("logged_in"):
            login_page()
        # If logged in but data not uploaded, show upload page
        elif not st.session_state.get("data_uploaded"):
            upload_page()


# Ensure session state is initialized
if "logged_in" not in st.session_state:
    st.session_state["logged_in"] = False

if "data_uploaded" not in st.session_state:
    st.session_state["data_uploaded"] = False

if "current_page" not in st.session_state:
    st.session_state["current_page"] = "Login"  # Default page is Login until logged in

# Render the appropriate page based on the session state
render_page()