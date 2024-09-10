-- Step 1: Create necessary temporary tables for Shipped With Cosmaline Reconciliation

-- Create erp_cosmaline
CREATE TEMPORARY TABLE IF NOT EXISTS erp_cosmaline AS
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

-- Create aramex_cosmaline_sum
CREATE TEMPORARY TABLE IF NOT EXISTS aramex_cosmaline_sum AS
SELECT
    Delivery_Date,
    SUM(
        CASE
            WHEN CODCurrency = 'LBP' THEN 
                COALESCE(CODAmount / (SELECT rate FROM daily_rate WHERE date = aramex.Delivery_Date), CODAmount / 89500)
            ELSE 
                O_CODAmount
        END
    ) AS sum_cod_amount_usd
FROM
    shippedandcollected_aramex_cosmaline aramex
WHERE
    TOKENNO = 'Shipped With Cosmaline'
GROUP BY
    Delivery_Date;

-- Create erp_cosmaline_sum
CREATE TEMPORARY TABLE IF NOT EXISTS erp_cosmaline_sum AS
SELECT
    receipt_date,
    SUM(receipt_amount_usd) AS sum_receipt_amount_usd
FROM
    erp_cosmaline
GROUP BY
    receipt_date;

-- Step 2: Insert into reconciliation_results table with duplicate check
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
    aramex_cosmaline_sum a ON e.receipt_date = a.Delivery_Date
WHERE NOT EXISTS (
    SELECT 1 FROM reconciliation_results rr
    WHERE rr.reference_date = e.receipt_date AND rr.reconciliation_type = 'Shipped With Cosmaline Reconciliation'
);

-- Clean up temporary tables
DROP TABLE IF EXISTS erp_cosmaline, aramex_cosmaline_sum, erp_cosmaline_sum;

COMMIT;
