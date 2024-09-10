# Automated Integration and Validation System for E-Commerce and ERP Data at Malia Group

## Project Overview

### Abstract
This report presents a comprehensive approach to tackle business problems faced by Malia Group. Malia Group, a group of 25 companies in multiple sectors such as foods/goods distribution and technology solutions, is facing challenges in reconciling their stream of data coming from multiple sources related to their Cosmaline business. This highlights the need for real-time monitoring of their financial transactions across sources. Additionally, a thorough business analysis of the company’s vast number of recorded transactions for their Cosmaline e-commerce business could provide the company with effective strategies to boost their performance.

To address these issues, an automated Extract, Transform, and Load (ETL) system was developed using Apache Airflow to eliminate the need for manual data entry. Reconciliation scripts were embedded into the automated system to reveal discrepancies between data sources. Reconciled data is stored in a permanent database ready for analysis. Post-reconciliation, Exploratory Data Analysis (EDA) and data mining analysis using models such as Apriori and Frequent Pattern Growth (FP-Growth) were employed to provide actionable insights for boosting sales. The reconciliation results highlighted an integration problem between e-commerce and ERP transactions and discrepancies between CyberSource and credit card data. Business analysis revealed customer preferences for smaller, more diverse carts valued under $20, suggesting opportunities for promoting low-performing products. Market basket analysis identified bundling and cross-selling opportunities targeting various segments.

This project equips Malia Group with a robust set of tools to resolve current data challenges, automates the entire business analysis pipeline, and positions the company for short-term sales boosts and long-term expansion.

**Keywords:** ETL, Automation, Reconciliation, Data Mining, Forecasting, Exploratory Data Analysis

## Methodology

### 3.1 Overview
Malia’s Project aims to enhance data integrity by creating an automated system for billing and collection processes for their Cosmaline e-commerce platform. The technology will align the company’s ERP system with its e-commerce transactions and shipping company data. Objectives include:

- **Errors Related to Manual Data Input**: Develop an automated ETL system to extract, transform, reconcile, and store data from multiple sources, addressing manual entry errors.
- **Data Reconciliation**: Use SQL scripts to verify transactions on predefined business rules, detecting matches and mismatches between sources.
- **Data Monitoring**: Create a Power BI dashboard to provide real-time monitoring and detailed transaction insights.
- **Model Building**: Employ forecasting models and market basket analysis to provide insights into customer patterns and improve e-commerce performance.

Docker was used for containerization and consistency across environments, Apache Airflow for workflow orchestration, MySQL for data storage, Python and R for processing and modeling, Power BI for visualization, and Streamlit for user interface development.

### 3.2 Data Collection and Sources
The project utilized eight main data sources related to Cosmaline’s e-commerce business:

1. **ECOM Data Website**: 10,389 rows, 9 columns. Details include order numbers, shipper names, dates, forms of billing, amounts, currencies, nations, and airway bills (AWB).
2. **Shipped & Collected – Aramex**: 8,810 rows, 8 columns. Details include shipper numbers, HAWB numbers, delivery dates, COD amounts, and invoice dates.
3. **Shipped & Collected – Cosmaline**: 1,638 rows, 4 columns. Details include shipper numbers, HAWB numbers, delivery dates, COD amounts, and invoice dates.
4. **Collected – Credit Card**: 2,205 rows, 4 columns. Includes order numbers, payment amounts, and payment dates.
5. **ERP-Oracle Collection**: 124 rows, 9 columns. Includes receipt number, currency code, exchange rate, customer details, receipt class, and collection source.
6. **Oracle Data**: 21,332 rows, 13 columns. Detailed representation of e-commerce orders spanning multiple rows, including product details, pricing, and customer information.
7. **Daily Rate**: 117 rows, 2 columns. Records daily Lebanese pound to USD exchange rates.
8. **Oracle Product Names**: 639 rows, 3 columns. Names and categories of products scraped from the Cosmaline website.

All datasets, except for Oracle Product Names, were supplied in Excel format and imported from a local directory. The Oracle Product Names dataset was acquired through web scraping.

### Table: Tools Used for the Project

| Technique/Tool        | Purpose                               | Details                                                                 |
|-----------------------|---------------------------------------|-------------------------------------------------------------------------|
| Docker                | Containerization and Environment Consistency | Hosts Apache Airflow, ensuring consistent deployment across environments. |
| Apache Airflow        | Workflow Orchestration                | Manages ETL processes, ensuring automated and efficient data workflows. |
| MySQL                 | Centralized Data Storage              | Primary database for storing e-commerce data, reconciliation results, etc. |
| Python/R              | Data Processing and Analysis          | Used for data cleaning, model development, and statistical analysis.   |
| Power BI              | Dashboarding and Data Visualization   | Interactive dashboards for real-time monitoring of key metrics.          |
| Apriori/FP-Growth     | Market Basket Analysis                | Algorithms used to discover associations in e-commerce transaction data. |
| Streamlit             | User Interface for Model Interaction  | Web app allowing users to interact with models and customize parameters. |
| Draw.io               | Informative Diagrams                  | Diagrams depicting various aspects of the system.                      |

## Results and Discussion

### Results
1. **Data Reconciliation**:
   - Significant discrepancies were found between e-commerce and ERP cash data, highlighting integration issues.
   - Discrepancies were identified between CyberSource and credit card data, indicating potential issues with payment processing or data entry.
   - E-commerce and Aramex data integration was generally strong, though some inconsistencies were noted in Cosmaline’s transactions.

2. **Exploratory Data Analysis**:
   - The analysis revealed customer preferences for smaller, diverse carts with an average value under $20.
   - Frequent Pattern Growth analysis identified popular product combinations and revealed opportunities for bundling.

3. **Forecasting**:
   - The ARIMA(1,1,1) model suggested stable revenue patterns, with a slight upward trend, indicating a need for strategic growth initiatives.

4. **Market Basket Analysis**:
   - Identified several strong associations between products, supporting strategies for bundling and cross-selling.
   - High confidence and lift values in association rules suggest reliable patterns for promotional activities.

### Discussion
- **Integration Issues**: The discrepancies between e-commerce and ERP data necessitate further investigation into the integration process. Improving API connections and ensuring data consistency can mitigate these issues.
- **Customer Insights**: The preference for smaller, diverse carts under $20 suggests that promotional strategies should focus on smaller bundles and product variety. Offering incentives for larger purchases could help increase average order value.
- **Forecasting and Growth**: The stable revenue forecast indicates that while current strategies are effective, exploring new markets or expanding product lines could support long-term growth.
- **Market Basket Analysis**: The identified associations provide actionable insights for product bundling and cross-selling. Implementing these strategies could enhance sales and customer satisfaction by offering relevant product combinations.

This comprehensive approach to data integration, reconciliation, and analysis positions Malia Group to resolve current challenges, optimize their e-commerce strategy, and achieve both short-term and long-term business objectives.
