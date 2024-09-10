from fpdf import FPDF
import os
import pandas as pd

class PDFReport(FPDF):
    def __init__(self, output_dir, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output_dir = output_dir
        
    def header(self):
        if self.page_no() > 1:  # Skip header on cover page
            self.image(os.path.join(self.output_dir, 'visuals/logos.png'), x=80, y=10, w=50)
            self.set_font('Times', 'B', 12)
            self.cell(0, 20, '', 0, 1, 'C')
            self.ln(10)

    def footer(self):
        self.set_y(-15)
        self.set_font('Times', 'I', 8)
        self.cell(0, 10, f'Page {self.page_no()}', 0, 0, 'C')

    def cover_page(self):
        self.add_page()
        self.set_font('Times', 'B', 20)
        self.cell(0, 80, '', 0, 1, 'C')  # Initial space before the title
        self.cell(0, 10, 'AUB x Malia Capstone E-commerce Performance Analysis', 0, 1, 'C')
        self.cell(0, 60, '', 0, 1, 'C')  # Space between the title and "Created by" statement
        self.set_font('Times', '', 12)
        self.cell(0, 10, 'Created by: Romanos Rizk and Hadil Fares', 0, 1, 'C')

    def chapter_title(self, title):
        self.set_font('Times', 'B', 16)
        self.cell(0, 10, title, 0, 1, 'L')
        self.ln(5)

    def chapter_body(self, body):
        self.set_font('Times', '', 12)
        self.multi_cell(0, 10, body)
        self.ln()

    def add_image(self, image_path, title, description):
        self.chapter_title(title)
        self.chapter_body(description)
        self.image(image_path, w=180)
        self.ln(10)

    def add_csv_as_table(self, file_path, title, description):
        self.chapter_title(title)
        self.chapter_body(description)
        df = pd.read_csv(file_path)
        col_widths = [max(self.get_string_width(str(value)) for value in df[col]) + 4 for col in df.columns]
        col_widths = [max(width, 40) for width in col_widths]  # Set a minimum column width
        self.set_font('Times', 'B', 10)
        for col, width in zip(df.columns, col_widths):
            self.cell(width, 10, col, border=1)
        self.ln()
        self.set_font('Times', '', 10)
        for i in range(len(df)):
            for value, width in zip(df.iloc[i], col_widths):
                self.cell(width, 10, str(value), border=1)
            self.ln()
        self.ln()

    def add_csv_as_paragraph(self, file_path, title, description):
        self.chapter_title(title)
        self.chapter_body(description)
        df = pd.read_csv(file_path)
        products = df['Product Name'].tolist()
        paragraph = ', '.join(products)
        self.chapter_body(paragraph)

def generate_pdf_report(output_dir, pdf_path):
    pdf = PDFReport(output_dir=output_dir)
    
    # Set the correct directory for images
    image_dir = os.path.join(output_dir, 'visuals')
    # Add Cover Page
    pdf.cover_page()

    # Section 1: Sales Analysis
    pdf.add_page()
    pdf.chapter_title('Section 1: Sales Analysis')
    pdf.add_image(os.path.join(image_dir, 'top_products.png'), 'Top 10 Best-Selling Products', 
                  "The 'Top 10 Best-Selling Products' plot shows the products with the highest sales volume. This visualization highlights the top-performing products in terms of quantity sold, providing insights into customer preferences and popular items.")
    pdf.add_image(os.path.join(image_dir, 'monthly_sales_trend.png'), 'Monthly Sales Trend', 
                  "The 'Monthly Sales Trend' plot displays the total sales revenue for each month. It helps in understanding the sales performance over time, identifying peak sales periods, and evaluating seasonal trends or patterns.")
    pdf.add_csv_as_table(os.path.join(image_dir, 'top_categories.csv'), 'Top Categories by Sales Volume', 
                         "The 'Top Categories by Sales Volume' table ranks product categories by their total sales volume. This information is useful for understanding which categories are most popular and contribute most significantly to overall sales.")
    pdf.add_image(os.path.join(image_dir, 'category_sales.png'), 'Revenue Contribution by Product Category', 
                  "The 'Revenue Contribution by Product Category' plot shows the percentage of total revenue generated by each product category. It highlights the most financially significant categories and helps identify which ones drive the most revenue.")
    pdf.add_image(os.path.join(image_dir, 'order_quantity_distribution.png'), 'Distribution of Order Quantities', 
                  "The 'Distribution of Order Quantities' plot is a histogram that visualizes the frequency of various order quantities placed by customers. It provides insight into purchasing behavior by showing how many items are typically ordered in a single transaction. This histogram helps identify common order sizes, revealing whether customers typically buy single items or larger quantities.")
    pdf.add_image(os.path.join(image_dir, 'monetary_distribution.png'), 'Monetary Distribution', 
                  "The 'Monetary Distribution' plot is a histogram that displays the distribution of total spending per order, showing the frequency of different spending levels by customers. It helps to understand the monetary value of transactions, indicating how much customers typically spend in a single purchase.")

    # Section 2: Order Analysis
    pdf.add_page()
    pdf.chapter_title('Section 2: Order Analysis')
    pdf.add_image(os.path.join(image_dir, 'recency_distribution.png'), 'Recency Distribution', 
                  "The 'Recency Distribution' plot is a histogram that illustrates the distribution of the number of days since customers last made a purchase. This plot provides insight into customer engagement by showing how recently customers have interacted with the business. The x-axis represents the number of days since the last purchase, while the y-axis shows the frequency of customers falling into each recency category. This analysis helps identify the proportion of recent versus infrequent customers, which can be useful for tailoring marketing and retention strategies.")
    pdf.add_csv_as_table(os.path.join(image_dir, 'order_quantity_stats.csv'), 'Order Quantity Statistics', 
                         "The 'Order Quantity Statistics' table presents summary statistics for the quantity of items ordered per transaction. It provides key metrics such as the average, minimum, maximum, median, and quartiles (Q1 and Q3) of the total items ordered in each transaction. This table offers a comprehensive overview of order sizes, highlighting typical order quantities, the range of quantities ordered, and the distribution's central tendency and spread. This information can help in understanding customer purchasing patterns.")
    pdf.add_image(os.path.join(image_dir, 'items_per_order_distribution.png'), 'Distribution of Items per Order', 
                  "The 'Distribution of Items per Order' plot is a histogram that visualizes the frequency of different quantities of items ordered in a single transaction. It shows how many transactions include specific numbers of items, providing insight into typical order sizes. This plot helps identify common order sizes, whether customers often purchase single items or multiple items, and the overall distribution of items per transaction.")

    # Section 3: Pricing and Discount Analysis
    pdf.add_page()
    pdf.chapter_title('Section 3: Pricing and Discount Analysis')
    pdf.add_image(os.path.join(image_dir, 'price_distribution.png'), 'Distribution of Selling Prices and List Prices', 
                  "The 'Distribution of Selling Prices and List Prices' plot is a histogram that shows the frequency distribution of both the selling prices and list prices of products. The plot helps to understand the range and distribution of prices at which products are sold (selling price) compared to their original or undiscounted prices (list price).")
    pdf.add_image(os.path.join(image_dir, 'discount_percentage_distribution.png'), 'Distribution of Discount Percentages', 
                  "The 'Distribution of Discount Percentages' plot is a histogram that displays the frequency distribution of discount percentages applied to products. It provides insight into the range and prevalence of discounts offered on products. This plot helps to identify common discount levels and the extent of price reductions across the product range.")
    pdf.add_image(os.path.join(image_dir, 'discount_percentage_by_category.png'), 'Discount Percentage by Category', 
                  "The 'Discount Percentage by Category' plot shows the variation in discount percentages across different product categories. This visualization helps to understand how discounting strategies differ between categories and which categories offer higher or lower discounts.")
    pdf.add_image(os.path.join(image_dir, 'discount_vs_ordered_quantity.png'), 'Scatter Plot of Discount Percentage vs. Ordered Quantity', 
                  "The 'Scatter Plot of Discount Percentage vs. Ordered Quantity' shows the relationship between the discount percentage applied to products and the quantity ordered. It helps to analyze whether higher discounts correlate with higher sales volumes, providing insights into the effectiveness of discount strategies.")
    pdf.add_image(os.path.join(image_dir, 'average_discounts_comparison_hist.png'), 'Comparison of Average Discounts for Popular and Less Popular Items', 
                  "The 'Comparison of Average Discounts for Popular and Less Popular Items' plot is a histogram that compares the average discount percentages applied to popular and less popular items. The items are categorized based on their sales volume, with 'popular' items having sales above the median and 'less popular' items below. This plot helps to understand how discount strategies vary based on product popularity, showing the distribution of average discounts for each category. It provides insight into whether popular or less popular items receive larger discounts on average.")
    pdf.add_image(os.path.join(image_dir, 'top_categories_unit_selling_prices.png'), 'Box Plot of Unit Selling Prices by Top Performing Product Categories', 
                  "The 'Box Plot of Unit Selling Prices by Top Performing Product Categories' visualizes the range and distribution of unit selling prices across the top-performing product categories. This plot provides insight into the pricing strategies within these categories, highlighting price variations and the median price point.")

    # Section 4: Supply Chain Analysis
    pdf.add_page()
    pdf.chapter_title('Section 4: Supply Chain Analysis')
    pdf.add_image(os.path.join(image_dir, 'top_distributors_treemap.png'), 'Top Distributing Companies by Quantity Shipped', 
                  "The 'Top Distributing Companies by Quantity Shipped' treemap visualizes the quantity of items shipped by each distributor. It provides a clear view of the most active distribution companies, showing their relative contribution to the total shipments. This visualization helps to understand the role and impact of different distributors in the supply chain.")

    # Section 5: Product Positioning
    pdf.add_page()
    pdf.chapter_title('Section 5: Product Positioning')
    pdf.add_image(os.path.join(image_dir, 'transaction_diversity.png'), 'Transaction Diversity', 
                  "The 'Transaction Diversity' plot shows the diversity in transactions by displaying the number of unique items purchased in each transaction. It helps to understand whether customers are buying a wide variety of products or focusing on a few specific items.")
    pdf.add_image(os.path.join(image_dir, 'item_frequencies.png'), 'Distribution of Item Frequencies', 
                  "The 'Distribution of Item Frequencies' plot shows how frequently each product is purchased. This plot provides insight into product popularity and helps identify which products are commonly bought together.")
    pdf.add_csv_as_table(os.path.join(image_dir, 'transaction_diversity_stats.csv'), 'Transaction Diversity Statistics', 
                         "The 'Transaction Diversity Statistics' table presents summary statistics regarding the number of unique items purchased per transaction. It provides key metrics such as the mean, median, minimum, maximum, and quartiles (Q1 and Q3) of the unique items in each transaction. This table offers an overview of how varied customer purchases are in terms of product diversity within a single transaction.")
    pdf.add_csv_as_table(os.path.join(image_dir, 'item_frequencies_stats.csv'), 'Item Frequency Statistics', 
                         "The 'Item Frequency Statistics' table provides summary statistics on how frequently each item is purchased across all transactions. It includes key metrics such as the mean, median, minimum, maximum, and quartiles (Q1 and Q3) for the frequency of item purchases. This table offers insights into the popularity of different products, highlighting how often items are bought on average, the range of purchase frequencies, and the distribution of these frequencies.")
    pdf.add_csv_as_paragraph(os.path.join(image_dir, 'combined_niche_products.csv'), 'Considered as Niche Products', 
                             "The 'Considered as Niche Products' section lists products identified as niche based on specific criteria. These products are less frequently purchased but may cater to specific customer segments or interests. Understanding niche products helps in targeting niche markets and optimizing inventory. Below is a block containing all Niche Products")

    # Output the PDF to a file
    pdf.output(pdf_path)
    print('PDF report created successfully.')