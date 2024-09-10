-- Step 1: Create and populate temporary tables

-- Drop temporary tables if they exist
-- DROP TEMPORARY TABLE IF EXISTS erp_data_tokens;
-- DROP TEMPORARY TABLE IF EXISTS aramex_cod_sum;
-- DROP TEMPORARY TABLE IF EXISTS erp_receipt_sum;
-- DROP TEMPORARY TABLE IF EXISTS erp_cosmaline;
-- DROP TEMPORARY TABLE IF EXISTS aramex_cosmaline_sum;
-- DROP TEMPORARY TABLE IF EXISTS erp_cosmaline_sum;
-- DROP TEMPORARY TABLE IF EXISTS erp_cybersource_sum;
-- DROP TEMPORARY TABLE IF EXISTS credit_card_sum;

-- Step 1: Create and populate temporary tables

-- Step 1.1: Create erp_data_tokens
CREATE TEMPORARY TABLE erp_data_tokens AS
SELECT
    receipt_number,
    CASE
        WHEN currency_code = 'LBP' THEN receipt_amount / exchange_rate
        ELSE receipt_amount
    END AS receipt_amount_usd,
    REGEXP_SUBSTR(comments, '[0-9]{7}[A-Z]{2}') AS token
FROM
    erp_data
WHERE
    comments REGEXP '[0-9]{7}[A-Z]{2}';

-- Step 1.2: Create aramex_cod_sum
CREATE TEMPORARY TABLE aramex_cod_sum AS
SELECT
    TOKENNO AS token,
    SUM(
    CASE
        WHEN CODCurrency = 'LBP' THEN 
            CODAmount / COALESCE((SELECT rate FROM daily_rate WHERE date = aramex.Delivery_Date), 89500)
        ELSE 
            O_CODAmount
    END
) AS sum_cod_amount_usd
FROM
    shippedandcollected_aramex_cosmaline aramex
GROUP BY
    TOKENNO;

-- Step 1.3: Create erp_receipt_sum
CREATE TEMPORARY TABLE erp_receipt_sum AS
SELECT
    token,
    SUM(receipt_amount_usd) AS sum_receipt_amount_usd
FROM
    erp_data_tokens
GROUP BY
    token;

-- Step 1.4: Create erp_cosmaline
CREATE TEMPORARY TABLE erp_cosmaline AS
SELECT
    receipt_date,
    CASE
        WHEN currency_code = 'LBP' THEN receipt_amount / exchange_rate
        ELSE receipt_amount
    END AS receipt_amount_usd
FROM
    erp_data
WHERE
    comments = 'Shipped With Cosmaline';

-- Step 1.5: Create aramex_cosmaline_sum
CREATE TEMPORARY TABLE aramex_cosmaline_sum AS
SELECT
    Delivery_Date,
    SUM(CASE
            WHEN CODCurrency = 'LBP' THEN COALESCE(
                CODAmount / (SELECT rate FROM daily_rate WHERE date = aramex.Delivery_Date),
                CODAmount / 89500)
            ELSE O_CODAmount
        END) AS sum_cod_amount_usd
FROM
    shippedandcollected_aramex_cosmaline aramex
WHERE
    TOKENNO = 'Shipped With Cosmaline'
GROUP BY
    Delivery_Date;

-- Step 1.6: Create erp_cosmaline_sum
CREATE TEMPORARY TABLE erp_cosmaline_sum AS
SELECT
    receipt_date,
    SUM(receipt_amount_usd) AS sum_receipt_amount_usd
FROM
    erp_cosmaline
GROUP BY
    receipt_date;

-- Step 1.7: Create erp_cybersource_sum
CREATE TEMPORARY TABLE erp_cybersource_sum AS
SELECT
    DATE(receipt_date) AS date,
    SUM(CASE
        WHEN currency_code = 'LBP' THEN receipt_amount / exchange_rate
        ELSE receipt_amount
    END) AS sum_receipt_amount_usd
FROM
    erp_data
WHERE
    comments = 'Cybersource'
GROUP BY
    DATE(receipt_date);

-- Step 1.8: Create credit_card_sum
CREATE TEMPORARY TABLE credit_card_sum AS
SELECT
    DATE(value_date) AS date,
    SUM(amount) AS sum_credit_card_amount
FROM
    credit_card
GROUP BY
    DATE(value_date);

-- Step 2: Populate the reconciliation_results table

-- Tokens Reconciliation
INSERT INTO reconciliation_results (reconciliation_type, reference_id, sum_erp_amount_usd, sum_shipping_amount_usd, reconciliation_status, non_existent_record, recon_report)
SELECT 
    'Tokens Reconciliation' AS reconciliation_type,
    e.token AS reference_id,
    e.sum_receipt_amount_usd,
    a.sum_cod_amount_usd,
    CASE
        WHEN ABS(e.sum_receipt_amount_usd - a.sum_cod_amount_usd) <= 0.05 * a.sum_cod_amount_usd THEN 'Match'
        ELSE 'Mismatch'
    END AS reconciliation_status,
    0 AS non_existent_record,
    CASE
        WHEN ABS(e.sum_receipt_amount_usd - a.sum_cod_amount_usd) <= 0.05 * a.sum_cod_amount_usd THEN 'Match'
        ELSE 'Mismatch due to different amounts'
    END AS recon_report
FROM 
    erp_receipt_sum e
LEFT JOIN 
    aramex_cod_sum a ON e.token = a.token;

-- Shipped With Cosmaline Reconciliation
INSERT INTO reconciliation_results (reconciliation_type, reference_date, sum_erp_amount_usd, sum_shipping_amount_usd, reconciliation_status, non_existent_record, recon_report)
SELECT 
    'Shipped With Cosmaline Reconciliation' AS reconciliation_type,
    e.receipt_date AS reference_date,
    e.sum_receipt_amount_usd,
    a.sum_cod_amount_usd,
    CASE
        WHEN ABS(e.sum_receipt_amount_usd - a.sum_cod_amount_usd) <= 5 THEN 'Match'
        ELSE 'Mismatch'
    END AS reconciliation_status,
    CASE 
        WHEN a.sum_cod_amount_usd IS NULL THEN 1
        ELSE 0
    END AS non_existent_record,
    CASE
        WHEN ABS(e.sum_receipt_amount_usd - a.sum_cod_amount_usd) <= 5 THEN 'Match'
        WHEN a.sum_cod_amount_usd IS NULL THEN 'Mismatch due to non-existent record'
        ELSE 'Mismatch due to different amounts'
    END AS recon_report
FROM 
    erp_cosmaline_sum e
LEFT JOIN 
    aramex_cosmaline_sum a ON e.receipt_date = a.Delivery_Date;

-- ECOM Orders Reconciliation
INSERT INTO reconciliation_results (reconciliation_type, reference_id, sum_erp_amount_usd, sum_shipping_amount_usd, reconciliation_status, non_existent_record, recon_report)
SELECT 
    'ECOM Orders Reconciliation' AS reconciliation_type,
    e.order_number AS reference_id,
    e.amount AS sum_erp_amount_usd,
    SUM(CASE 
           WHEN a.CODCurrency = 'USD' THEN a.O_CODAmount
           WHEN a.CODCurrency = 'LBP' THEN a.CODAmount / COALESCE((SELECT rate 
                                                                   FROM daily_rate 
                                                                   WHERE date = a.Delivery_Date), 89500)
           ELSE 0
       END) AS sum_cod_amount_usd,
    CASE
        WHEN ABS(e.amount - SUM(CASE 
                                   WHEN a.CODCurrency = 'USD' THEN a.O_CODAmount
                                   WHEN a.CODCurrency = 'LBP' THEN a.CODAmount / COALESCE((SELECT rate 
                                                                                           FROM daily_rate 
                                                                                           WHERE date = a.Delivery_Date), 89500)
                                   ELSE 0
                               END)) <= 1 THEN 'Match'
        ELSE 'Mismatch'
    END AS reconciliation_status,
    CASE
        WHEN COUNT(a.order_number) = 0 THEN 1
        ELSE 0
    END AS non_existent_record,
    CASE
        WHEN ABS(e.amount - SUM(CASE 
                                   WHEN a.CODCurrency = 'USD' THEN a.O_CODAmount
                                   WHEN a.CODCurrency = 'LBP' THEN a.CODAmount / COALESCE((SELECT rate 
                                                                                           FROM daily_rate 
                                                                                           WHERE date = a.Delivery_Date), 89500)
                                   ELSE 0
                               END)) <= 1 THEN 'Match'
        WHEN COUNT(a.order_number) = 0 THEN 'Mismatch due to non-existent record'
        ELSE 'Mismatch due to different amounts'
    END AS recon_report
FROM 
    ecom_orders e
LEFT JOIN 
    shippedandcollected_aramex_cosmaline a ON e.order_number = a.order_number
GROUP BY 
    e.order_number;

-- Cybersource Reconciliation
INSERT INTO reconciliation_results (reconciliation_type, reference_date, sum_erp_amount_usd, sum_shipping_amount_usd, reconciliation_status, non_existent_record, recon_report)
SELECT 
    'Cybersource Reconciliation' AS reconciliation_type,
    e.date AS reference_date,
    e.sum_receipt_amount_usd,
    c.sum_credit_card_amount AS sum_shipping_amount_usd,
    CASE
        WHEN ABS(e.sum_receipt_amount_usd - c.sum_credit_card_amount) <= 0.05 * c.sum_credit_card_amount THEN 'Match'
        ELSE 'Mismatch'
    END AS reconciliation_status,
    0 AS non_existent_record,
    CASE
        WHEN ABS(e.sum_receipt_amount_usd - c.sum_credit_card_amount) <= 0.05 * c.sum_credit_card_amount THEN 'Match'
        ELSE 'Mismatch due to different amounts'
    END AS recon_report
FROM 
    erp_cybersource_sum e
LEFT JOIN 
    credit_card_sum c ON e.date = c.date;

-- ECOM Orders not in Shipping
INSERT INTO reconciliation_results (reconciliation_type, reference_id, reconciliation_status, non_existent_record, recon_report)
SELECT 
    'ECOM Orders not in Shipping' AS reconciliation_type,
    e.order_number AS reference_id,
    'Mismatch' AS reconciliation_status,
    1 AS non_existent_record,
    'Mismatch due to non-existent record' AS recon_report
FROM 
    ecom_orders e
LEFT JOIN 
    shippedandcollected_aramex_cosmaline a ON e.order_number = a.order_number
WHERE 
    a.order_number IS NULL;

-- ECOM Orders not in Oracle
INSERT INTO reconciliation_results (reconciliation_type, reference_id, reconciliation_status, non_existent_record, recon_report)
SELECT 
    'ECOM Orders not in Oracle' AS reconciliation_type,
    e.order_number AS reference_id,
    'Mismatch' AS reconciliation_status,
    1 AS non_existent_record,
    'Mismatch due to non-existent record' AS recon_report
FROM 
    ecom_orders e
LEFT JOIN 
    oracle_data o ON e.order_number = o.ecom_reference_order_number
WHERE 
    o.ecom_reference_order_number IS NULL;

-- Invalid Shipping Order Numbers
INSERT INTO reconciliation_results (reconciliation_type, reference_id, reconciliation_status, non_existent_record, recon_report)
SELECT 
    'Invalid Shipping Order Numbers' AS reconciliation_type,
    a.order_number AS reference_id,
    'Mismatch' AS reconciliation_status,
    1 AS non_existent_record,
    'Mismatch due to non-existent record' AS recon_report
FROM 
    shippedandcollected_aramex_cosmaline a
LEFT JOIN 
    ecom_orders e ON a.order_number = e.order_number
WHERE 
    e.order_number IS NULL AND a.order_number IS NOT NULL;

-- Invalid Oracle Order Numbers
INSERT INTO reconciliation_results (reconciliation_type, reference_id, reconciliation_status, non_existent_record, recon_report)
SELECT 
    'Invalid Oracle Order Numbers' AS reconciliation_type,
    o.ecom_reference_order_number AS reference_id,
    'Mismatch' AS reconciliation_status,
    1 AS non_existent_record,
    'Mismatch due to non-existent record' AS recon_report
FROM 
    oracle_data o
LEFT JOIN 
    ecom_orders e ON o.ecom_reference_order_number = e.order_number
WHERE 
    e.order_number IS NULL AND o.ecom_reference_order_number IS NOT NULL;

COMMIT;