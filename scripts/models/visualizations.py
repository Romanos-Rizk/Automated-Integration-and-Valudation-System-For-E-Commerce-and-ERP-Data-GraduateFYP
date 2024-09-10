import pandas as pd
import ast
import re
import matplotlib.pyplot as plt
import seaborn as sns
import squarify
import os

def save_summary_statistics(df, file_path):
    summary_stats = df[['ordered_quantity', 'unit_selling_price', 'unit_list_price']].describe()
    with open(file_path, 'w') as f:
        f.write("Summary Statistics for Numerical Columns:\n")
        f.write(summary_stats.to_string())
    print(f"Summary statistics saved to {file_path}")
    
def save_categorical_distribution(df, column_name, file_path):
    categorical_dist = df[column_name].value_counts()
    with open(file_path, 'w') as f:
        f.write(f"Distribution of {column_name}:\n")
        f.write(categorical_dist.to_string())
    print(f"Categorical distribution saved to {file_path}")
    
def save_unique_categories_info(df, column_name, file_path):
    # Split the comma-separated strings into individual categories and create a set of unique categories
    unique_categories = set()
    df[column_name].apply(lambda x: unique_categories.update(x.split(', ')))

    # Count the number of unique categories
    num_unique_categories = len(unique_categories)
    
    # Save the results to a text file
    with open(file_path, 'w') as f:
        f.write(f"Number of unique categories: {num_unique_categories}\n")
        f.write(f"Unique categories: {unique_categories}\n")
    
    print(f"Unique categories information saved to {file_path}")
    
def plot_top_products(df, file_path):
    # Filter out "Product Not Found" and calculate the top 10 best-selling products
    top_products = df[df['product_name'] != 'Product Not Found']\
                    .groupby('product_name')['ordered_quantity']\
                    .sum().sort_values(ascending=False).head(10)
    
    # Plot the top 10 best-selling products
    plt.figure(figsize=(12, 6))
    top_products.plot(kind='bar')
    plt.title('Top 10 Best-Selling Products (Excluding "Product Not Found")')
    plt.xlabel('Product Name')
    plt.ylabel('Total Quantity Sold')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()  # Adjust layout to ensure everything fits without overlapping
    plt.savefig(file_path)
    plt.close()
    print(f"Top products plot saved to {file_path}")

def plot_monthly_sales_trend(df, file_path):
    # Ensure that the 'ordered_date' column is in datetime format
    if not pd.api.types.is_datetime64_any_dtype(df['ordered_date']):
        df['ordered_date'] = pd.to_datetime(df['ordered_date'])
    # Extract month and year from ordered_date
    df['year_month'] = df['ordered_date'].dt.to_period('M')

    # Aggregate sales by month
    monthly_sales = df.groupby('year_month')['total_sales_with_discount'].sum()

    # Convert the year_month to a string format for better readability
    monthly_sales.index = monthly_sales.index.strftime('%B %Y')

    # Plot the monthly sales trend as a horizontal bar chart
    plt.figure(figsize=(12, 6))
    monthly_sales.plot(kind='barh')
    plt.title('Bar Chart of Monthly Sales')
    plt.xlabel('Total Sales with Discount')
    plt.ylabel('Month')
    plt.grid(True)
    plt.tight_layout()  # Adjust layout to ensure everything fits without overlapping
    plt.savefig(file_path)
    plt.close()
    print(f"Monthly sales trend plot saved to {file_path}")
    
def get_top_categories(df, file_path):
    # Group by product_category and sum the ordered_quantity
    category_quantity = df.groupby('product_category')['ordered_quantity'].sum().reset_index()

    # Sort the categories by total quantity sold and select the top 10
    top_categories = category_quantity.sort_values(by='ordered_quantity', ascending=False).head(10)

    # Save the top categories to a CSV file
    top_categories.to_csv(file_path, index=False)
    print(f"Top categories information saved to {file_path}")

def plot_category_sales(df, file_path):
    # Calculate total sales by product category
    category_sales = df.groupby('product_category')['total_sales_with_discount'].sum().sort_values(ascending=False)

    # Plot the revenue contribution by product category
    plt.figure(figsize=(12, 6))
    category_sales.plot(kind='bar')
    plt.title('Revenue Contribution by Product Category')
    plt.xlabel('Product Category')
    plt.ylabel('Total Sales')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()  # Adjust layout to ensure everything fits without overlapping
    plt.savefig(file_path)
    plt.close()
    print(f"Category sales plot saved to {file_path}")

def plot_order_quantity_distribution(df, file_path):
    # Plot distribution of order quantities
    plt.figure(figsize=(12, 6))
    df['ordered_quantity'].plot(kind='hist', bins=30)
    plt.title('Distribution of Order Quantities')
    plt.xlabel('Order Quantity')
    plt.ylabel('Frequency')

    # Set the x-axis ticks with increments of 5
    max_order_quantity = df['ordered_quantity'].max()
    plt.xticks(range(0, max_order_quantity + 1, 5))

    plt.tight_layout()
    plt.savefig(file_path)
    plt.close()
    print(f"Order quantity distribution plot saved to {file_path}")

def plot_monetary_distribution(df, file_path):
    # Calculate Monetary Value
    monetary = df.groupby('ecom_reference_order_number')['total_sales_with_discount'].sum()
    rfm = pd.DataFrame({'monetary': monetary})

    # Plot the Monetary Distribution
    plt.figure(figsize=(12, 6))
    sns.histplot(rfm, x='monetary', kde=True)
    plt.title('Monetary Distribution')
    plt.xlabel('Total Spend')
    plt.ylabel('Frequency')
    plt.tight_layout()
    plt.savefig(file_path)
    plt.close()
    print(f"Monetary distribution plot saved to {file_path}")

def plot_recency_distribution(df, file_path):
    # Calculate Recency
    df['recency'] = (df['ordered_date'].max() - df['ordered_date']).dt.days

    rfm = pd.DataFrame({
        'recency': df.groupby('ecom_reference_order_number')['recency'].min()
    })

    # Plot Recency Distribution
    plt.figure(figsize=(12, 6))
    sns.histplot(rfm, x='recency', kde=True)
    plt.title('Recency Distribution')
    plt.xlabel('Days Since Last Purchase')
    plt.ylabel('Frequency')
    plt.tight_layout()
    plt.savefig(file_path)
    plt.close()
    print(f"Recency distribution plot saved to {file_path}")

def save_order_quantity_stats(df, file_path):
    # Group by ecom_reference_order_number and calculate the sum of ordered_quantity for each order
    order_quantity_stats = df.groupby('ecom_reference_order_number')['ordered_quantity'].sum()

    # Calculate summary statistics
    summary_stats = order_quantity_stats.describe()

    # Extract the specific statistics and create a DataFrame
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

    # Save the results to a CSV file
    stats_df.to_csv(file_path, index=False)
    print(f"Order quantity statistics saved to {file_path}")

def plot_items_per_order_distribution(df, file_path):
    # Group by ecom_reference_order_number and calculate the sum of ordered_quantity for each order
    order_quantity_stats = df.groupby('ecom_reference_order_number')['ordered_quantity'].sum()

    # Plotting the histogram
    plt.figure(figsize=(12, 6))
    order_quantity_stats.plot(kind='hist', bins=30, alpha=0.7)
    plt.title('Distribution of Items per Order')
    plt.xlabel('Number of Items per Order')
    plt.ylabel('Frequency')
    plt.tight_layout()
    plt.savefig(file_path)
    plt.close()
    print(f"Items per order distribution plot saved to {file_path}")

def plot_price_distribution(df, file_path):
    # Plot distribution of unit selling prices and unit list prices
    plt.figure(figsize=(12, 6))
    df['unit_selling_price'].plot(kind='hist', bins=20, alpha=0.5, label='Selling Price')
    df['unit_list_price'].plot(kind='hist', bins=20, alpha=0.5, label='List Price')
    plt.title('Distribution of Selling Prices and List Prices')
    plt.xlabel('Price')
    plt.ylabel('Frequency')
    plt.legend()
    plt.tight_layout()
    plt.savefig(file_path)
    plt.close()
    print(f"Price distribution plot saved to {file_path}")

def plot_discount_percentage_distribution(df, file_path):
    # Calculate discount percentage
    df['discount_percentage'] = (df['unit_list_price'] - df['unit_selling_price']) / df['unit_list_price'] * 100

    # Plot the distribution of discount percentages
    plt.figure(figsize=(12, 6))
    df['discount_percentage'].plot(kind='hist', bins=20)
    plt.title('Distribution of Discount Percentages')
    plt.xlabel('Discount Percentage')
    plt.ylabel('Frequency')
    plt.tight_layout()
    plt.savefig(file_path)
    plt.close()
    print(f"Discount percentage distribution plot saved to {file_path}")

def plot_discount_percentage_by_category(df_expanded, file_path):
    # Calculate the total sales per category
    category_quantity = df_expanded.groupby('product_category')['ordered_quantity'].sum().reset_index()

    # Sort the categories by total sales
    sorted_categories = category_quantity.sort_values(by='ordered_quantity', ascending=False)['product_category']

    # Convert product_category to a categorical type with the sorted order
    df_expanded['product_category'] = pd.Categorical(df_expanded['product_category'], categories=sorted_categories, ordered=True)

    # Plot the box plot with sorted categories
    plt.figure(figsize=(15, 8))  # Increase figure width
    sns.boxplot(x='product_category', y='discount_percentage', data=df_expanded, width=0.5)
    plt.title('Box Plot of Discount Percentages by Product Category')
    plt.xlabel('Product Category')
    plt.ylabel('Discount Percentage')
    plt.xticks(rotation=90, ha='right')  # Rotate labels more and align to the right
    plt.tight_layout()  # Adjust layout to prevent clipping
    plt.savefig(file_path)
    plt.close()
    print(f"Discount percentages by category box plot saved to {file_path}")

def plot_discount_vs_ordered_quantity(df, file_path):
    # Calculate the correlation
    correlation = df['discount_percentage'].corr(df['ordered_quantity'])

    # Scatter plot of discount percentage vs. ordered quantity
    plt.figure(figsize=(12, 6))
    sns.scatterplot(x='discount_percentage', y='ordered_quantity', data=df, alpha=0.6)
    plt.title('Scatter Plot of Discount Percentage vs. Ordered Quantity')
    plt.xlabel('Discount Percentage')
    plt.ylabel('Ordered Quantity')
    plt.grid(True)

    # Add the correlation value to the plot
    plt.text(0.05, max(df['ordered_quantity']) * 0.95, f'Correlation: {correlation:.2f}', fontsize=12, color='red')

    plt.tight_layout()
    plt.savefig(file_path)
    plt.close()
    print(f"Scatter plot of discount percentage vs. ordered quantity saved to {file_path}")

def plot_average_discounts_comparison_hist(df, file_path_hist):
    # Calculate total quantity ordered for each item
    item_popularity = df.groupby('product_name')['ordered_quantity'].sum()

    # Determine the median popularity
    median_popularity = item_popularity.median()

    # Categorize items into popular and less popular
    df['popularity'] = df['product_name'].map(lambda x: 'Popular' if item_popularity[x] > median_popularity else 'Less Popular')

    # Calculate the average discount percentage for each item
    average_discounts = df.groupby('product_name')['discount_percentage'].mean()
    df['average_discount'] = df['product_name'].map(average_discounts)

    # Create a DataFrame for comparison
    comparison_df = df[['product_name', 'popularity', 'average_discount']].drop_duplicates()

    # Histogram to compare average discounts
    plt.figure(figsize=(12, 6))
    sns.histplot(data=comparison_df, x='average_discount', hue='popularity', element='step', stat='density', common_norm=False, bins=20)
    plt.title('Comparison of Average Discounts for Popular and Less Popular Items')
    plt.xlabel('Average Discount Percentage')
    plt.ylabel('Density')
    plt.tight_layout()
    plt.savefig(file_path_hist)
    plt.close()
    print(f"Histogram of average discounts comparison saved to {file_path_hist}")

def save_average_discounts_summary_stats(df, file_path_stats):
    # Calculate total quantity ordered for each item
    item_popularity = df.groupby('product_name')['ordered_quantity'].sum()

    # Determine the median popularity
    median_popularity = item_popularity.median()

    # Categorize items into popular and less popular
    df['popularity'] = df['product_name'].map(lambda x: 'Popular' if item_popularity[x] > median_popularity else 'Less Popular')

    # Calculate the average discount percentage for each item
    average_discounts = df.groupby('product_name')['discount_percentage'].mean()
    df['average_discount'] = df['product_name'].map(average_discounts)

    # Create a DataFrame for comparison
    comparison_df = df[['product_name', 'popularity', 'average_discount']].drop_duplicates()

    # Print summary statistics for further insight
    summary_stats = comparison_df.groupby('popularity')['average_discount'].describe()
    with open(file_path_stats, 'w') as f:
        f.write(summary_stats.to_string())
    print(f"Summary statistics saved to {file_path_stats}")

def plot_top_categories_unit_selling_prices(df_expanded, file_path):
    # Calculate total sales for each category
    df_expanded['total_sales_with_discount'] = df_expanded['ordered_quantity'] * df_expanded['unit_selling_price']
    category_sales = df_expanded.groupby('product_category')['total_sales_with_discount'].sum()

    # Identify top performing categories
    top_categories = category_sales.sort_values(ascending=False).head(10).index.tolist()

    # Filter DataFrame to include only top performing categories
    top_categories_df = df_expanded[df_expanded['product_category'].isin(top_categories)]

    # Plot the box plot for top performing categories with specified order
    plt.figure(figsize=(14, 8))
    sns.boxplot(x='product_category', y='unit_selling_price', data=top_categories_df, order=top_categories)
    plt.title('Box Plot of Unit Selling Prices by Top Performing Product Categories')
    plt.xlabel('Product Category')
    plt.ylabel('Unit Selling Price')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(file_path)
    plt.close()
    print(f"Box plot of unit selling prices by top performing product categories saved to {file_path}")
    
def plot_top_distributors_treemap(df, file_path):
    # Top distributing companies
    top_distributors = df.groupby('operating_unit_name')['ordered_quantity'].sum().sort_values(ascending=False).head(10)

    # Create a list of colors in shades of blue and reverse it
    colors = plt.cm.Blues(range(0, 256, int(256/len(top_distributors))))
    colors = colors[::-1]  # Reverse the colors

    # Create a treemap
    plt.figure(figsize=(12, 6))
    squarify.plot(sizes=top_distributors.values, label=top_distributors.index, alpha=0.8, color=colors)
    plt.title('Top Distributing Companies by Quantity Shipped')
    plt.axis('off')  # Remove axes
    plt.tight_layout()
    plt.savefig(file_path)
    plt.close()
    print(f"Treemap of top distributing companies saved to {file_path}")

def plot_transaction_diversity(df, file_path):
    # Create a basket matrix (binary matrix)
    basket = (df
              .groupby(['ecom_reference_order_number', 'product_name'])['product_name']
              .count().unstack().reset_index().fillna(0)
              .set_index('ecom_reference_order_number'))
    basket_sets = basket.applymap(lambda x: 1 if x > 0 else 0)

    # Count the number of unique items per transaction
    unique_items_per_transaction = basket_sets.sum(axis=1)

    # Plot the distribution of the number of items per transaction
    plt.figure(figsize=(10, 6))
    plt.hist(unique_items_per_transaction, bins=range(1, unique_items_per_transaction.max() + 1), edgecolor='k')
    plt.title('Distribution of the Number of Items per Transaction')
    plt.xlabel('Number of Items per Transaction')
    plt.ylabel('Frequency')
    plt.tight_layout()
    plt.savefig(file_path)
    plt.close()
    print(f"Distribution of the number of items per transaction plot saved to {file_path}")

def plot_item_frequencies(df, file_path):
    # Create a basket matrix (binary matrix)
    basket = (df
              .groupby(['ecom_reference_order_number', 'product_name'])['product_name']
              .count().unstack().reset_index().fillna(0)
              .set_index('ecom_reference_order_number'))
    basket_sets = basket.applymap(lambda x: 1 if x > 0 else 0)

    # Count the frequency of each item across all transactions
    item_frequencies = basket_sets.sum(axis=0)

    # Plot the distribution of item frequencies
    plt.figure(figsize=(10, 6))
    plt.hist(item_frequencies, bins=range(1, item_frequencies.max() + 1), edgecolor='k')
    plt.title('Distribution of Item Frequencies')
    plt.xlabel('Frequency of Items')
    plt.ylabel('Number of Items')
    plt.tight_layout()
    plt.savefig(file_path)
    plt.close()
    print(f"Distribution of item frequencies plot saved to {file_path}")

def save_transaction_diversity_stats(df, file_path):
    # Create a basket matrix (binary matrix)
    basket = (df
              .groupby(['ecom_reference_order_number', 'product_name'])['product_name']
              .count().unstack().reset_index().fillna(0)
              .set_index('ecom_reference_order_number'))
    basket_sets = basket.applymap(lambda x: 1 if x > 0 else 0)

    # Count the number of unique items per transaction
    unique_items_per_transaction = basket_sets.sum(axis=1)

    # Calculate basic statistics
    stats = unique_items_per_transaction.describe()

    # Convert statistics to DataFrame
    stats_df = pd.DataFrame(stats, columns=['Value']).reset_index()
    stats_df.columns = ['Statistic', 'Value']

    # Save the results to a CSV file
    stats_df.to_csv(file_path, index=False)
    print(f"Transaction diversity statistics saved to {file_path}")

def save_item_frequencies_stats(df, file_path):
    # Create a basket matrix (binary matrix)
    basket = (df
              .groupby(['ecom_reference_order_number', 'product_name'])['product_name']
              .count().unstack().reset_index().fillna(0)
              .set_index('ecom_reference_order_number'))
    basket_sets = basket.applymap(lambda x: 1 if x > 0 else 0)

    # Count the frequency of each item across all transactions
    item_frequencies = basket_sets.sum(axis=0)

    # Calculate basic statistics
    stats = item_frequencies.describe()

    # Convert statistics to DataFrame
    stats_df = pd.DataFrame(stats, columns=['Value']).reset_index()
    stats_df.columns = ['Statistic', 'Value']

    # Save the results to a CSV file
    stats_df.to_csv(file_path, index=False)
    print(f"Item frequency statistics saved to {file_path}")
    
def save_niche_products(df, file_path):
    # Calculate sales volume for each product
    sales_volume = df.groupby('product_name')['ordered_quantity'].sum().reset_index()

    # Identify products with lower sales volumes
    niche_products = sales_volume[sales_volume['ordered_quantity'] < sales_volume['ordered_quantity'].quantile(0.25)]

    # Save the niche products to a text file
    with open(file_path, 'w') as f:
        f.write("Potential Niche Products based on Sales Volume:\n")
        f.write(niche_products.to_string(index=False))
    print(f"Niche products information saved to {file_path}")

def save_niche_purchase_frequency(df, file_path):
    # Calculate purchase frequency for each product
    purchase_frequency = df.groupby('product_name')['ecom_reference_order_number'].nunique().reset_index()

    # Identify products with lower purchase frequency
    niche_purchase_frequency = purchase_frequency[purchase_frequency['ecom_reference_order_number'] < purchase_frequency['ecom_reference_order_number'].quantile(0.25)]

    # Save the niche purchase frequency products to a text file
    with open(file_path, 'w') as f:
        f.write("Potential Niche Products based on Purchase Frequency:\n")
        f.write(niche_purchase_frequency.to_string(index=False))
    print(f"Niche purchase frequency information saved to {file_path}")

def save_combined_niche_products(df, file_path):
    # Calculate sales volume for each product
    sales_volume = df.groupby('product_name')['ordered_quantity'].sum().reset_index()
    niche_products = sales_volume[sales_volume['ordered_quantity'] < sales_volume['ordered_quantity'].quantile(0.25)]
    
    # Calculate purchase frequency for each product
    purchase_frequency = df.groupby('product_name')['ecom_reference_order_number'].nunique().reset_index()
    niche_purchase_frequency = purchase_frequency[purchase_frequency['ecom_reference_order_number'] < purchase_frequency['ecom_reference_order_number'].quantile(0.25)]

    # Combine niche products based on different criteria
    combined_niche_products = set(niche_products['product_name']).intersection(
        set(niche_purchase_frequency['product_name'])
    )

    # Create a DataFrame for the combined niche products
    combined_niche_products_df = pd.DataFrame({
        'Product Name': list(combined_niche_products)
    })

    # Save the DataFrame to a CSV file
    combined_niche_products_df.to_csv(file_path, index=False)
    print(f"Combined niche products information saved to {file_path}")

def save_combined_niche_products_count(df, file_path):
    # Calculate sales volume for each product
    sales_volume = df.groupby('product_name')['ordered_quantity'].sum().reset_index()
    niche_products = sales_volume[sales_volume['ordered_quantity'] < sales_volume['ordered_quantity'].quantile(0.25)]
    
    # Calculate purchase frequency for each product
    purchase_frequency = df.groupby('product_name')['ecom_reference_order_number'].nunique().reset_index()
    niche_purchase_frequency = purchase_frequency[purchase_frequency['ecom_reference_order_number'] < purchase_frequency['ecom_reference_order_number'].quantile(0.25)]

    # Combine niche products based on different criteria
    combined_niche_products = set(niche_products['product_name']).intersection(
        set(niche_purchase_frequency['product_name'])
    )

    # Count the number of combined niche products
    num_combined_niche_products = len(combined_niche_products)

    # Create a DataFrame for the combined niche products
    combined_niche_products_df = pd.DataFrame({
        'Product Name': list(combined_niche_products)
    })

    # Add the count to the DataFrame
    combined_niche_products_df['Count'] = num_combined_niche_products

    # Save the DataFrame to a CSV file
    combined_niche_products_df.to_csv(file_path, index=False)
    print(f"Combined niche products count and information saved to {file_path}")
    
        
def generate_all_visualizations(df, df_expanded, output_dir):
    import os

    # Ensure the output directory exists
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # List of visualization functions using df
    visualization_functions_df = [
        (save_summary_statistics, df, 'summary_statistics.csv'),
        (save_categorical_distribution, df, 'operating_unit_name', 'categorical_distribution.csv'),
        (save_unique_categories_info, df, 'product_category', 'unique_categories_info.csv'),
        (plot_top_products, df, 'top_products.png'),
        (plot_monthly_sales_trend, df, 'monthly_sales_trend.png'),
        (plot_order_quantity_distribution, df, 'order_quantity_distribution.png'),
        (plot_monetary_distribution, df, 'monetary_distribution.png'),
        (plot_recency_distribution, df, 'recency_distribution.png'),
        (save_order_quantity_stats, df, 'order_quantity_stats.csv'),
        (plot_items_per_order_distribution, df, 'items_per_order_distribution.png'),
        (plot_price_distribution, df, 'price_distribution.png'),
        (plot_discount_percentage_distribution, df, 'discount_percentage_distribution.png'),
        (plot_discount_vs_ordered_quantity, df, 'discount_vs_ordered_quantity.png'),
        (plot_average_discounts_comparison_hist, df, 'average_discounts_comparison_hist.png'),
        (save_average_discounts_summary_stats, df, 'average_discounts_summary_stats.csv'),
        (plot_top_distributors_treemap, df, 'top_distributors_treemap.png'),
        (plot_transaction_diversity, df, 'transaction_diversity.png'),
        (plot_item_frequencies, df, 'item_frequencies.png'),
        (save_transaction_diversity_stats, df, 'transaction_diversity_stats.csv'),
        (save_item_frequencies_stats, df, 'item_frequencies_stats.csv'),
        (save_niche_products, df, 'niche_products.csv'),
        (save_niche_purchase_frequency, df, 'niche_purchase_frequency.csv'),
        (save_combined_niche_products, df, 'combined_niche_products.csv'),
        (save_combined_niche_products_count, df, 'combined_niche_products_count.csv'),
    ]

    # List of visualization functions using df_expanded
    visualization_functions_df_expanded = [
        (plot_discount_percentage_by_category, df_expanded, 'discount_percentage_by_category.png'),
        (plot_top_categories_unit_selling_prices, df_expanded, 'top_categories_unit_selling_prices.png'),
        (get_top_categories, df_expanded, 'top_categories.csv'),
        (plot_category_sales, df_expanded, 'category_sales.png'),
    ]

    # Call each visualization function with df
    for func, *args in visualization_functions_df:
        file_name = args[-1]
        file_path = os.path.join(output_dir, file_name)
        func(*args[:-1], file_path)  # Pass the args without the last element (file_name) and append file_path

    # Call each visualization function with df_expanded
    for func, *args in visualization_functions_df_expanded:
        file_name = args[-1]
        file_path = os.path.join(output_dir, file_name)
        func(*args[:-1], file_path)  # Pass the args without the last element (file_name) and append file_path



          
