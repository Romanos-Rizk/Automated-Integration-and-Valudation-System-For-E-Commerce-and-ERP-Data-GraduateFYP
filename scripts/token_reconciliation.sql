-- Step 1: Create necessary temporary tables for Tokens Reconciliation

-- Create erp_data_tokens
CREATE TEMPORARY TABLE IF NOT EXISTS erp_data_tokens AS
SELECT
    receipt_number,
    CASE
        WHEN currency_code = 'LBP' THEN receipt_amount / exchange_rate
        ELSE receipt_amount
    END AS receipt_amount_usd,
    REGEXP_SUBSTR(comments, '[0-9]{6}[A-Z]{2}|[0-9]{7}[A-Z]{2}') AS token
FROM
    erp_data
WHERE
    comments REGEXP '[0-9]{6}[A-Z]{2}|[0-9]{7}[A-Z]{2}';

-- Create aramex_cod_sum
CREATE TEMPORARY TABLE IF NOT EXISTS aramex_cod_sum AS
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

-- Create erp_receipt_sum
CREATE TEMPORARY TABLE IF NOT EXISTS erp_receipt_sum AS
SELECT
    token,
    SUM(receipt_amount_usd) AS sum_receipt_amount_usd
FROM
    erp_data_tokens
GROUP BY
    token;

-- Step 2: Insert into reconciliation_results table with duplicate check
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
    aramex_cod_sum a ON e.token = a.token
WHERE NOT EXISTS (
    SELECT 1 FROM reconciliation_results rr
    WHERE rr.reference_id = e.token AND rr.reconciliation_type = 'Tokens Reconciliation'
);

-- Clean up temporary tables
DROP TABLE IF EXISTS erp_data_tokens, aramex_cod_sum, erp_receipt_sum;

COMMIT;
