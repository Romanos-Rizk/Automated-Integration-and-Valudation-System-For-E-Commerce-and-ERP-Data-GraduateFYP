-- Step 1: Create necessary temporary tables for Cybersource Reconciliation

-- Create erp_cybersource_sum
CREATE TEMPORARY TABLE IF NOT EXISTS erp_cybersource_sum AS
SELECT
    DATE(receipt_date) AS date,
    SUM(
        CASE
            WHEN currency_code = 'LBP' THEN receipt_amount / exchange_rate
            ELSE receipt_amount
        END
    ) AS sum_receipt_amount_usd
FROM
    erp_data
WHERE
    comments = 'Cybersource'
GROUP BY
    DATE(receipt_date);

-- Create credit_card_sum
CREATE TEMPORARY TABLE IF NOT EXISTS credit_card_sum AS
SELECT
    DATE(value_date) AS date,
    SUM(amount) AS sum_credit_card_amount
FROM
    credit_card
GROUP BY
    DATE(value_date);

-- Step 2: Insert into reconciliation_results table with duplicate check
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
    credit_card_sum c ON e.date = c.date
WHERE NOT EXISTS (
    SELECT 1 FROM reconciliation_results rr
    WHERE rr.reference_date = e.date AND rr.reconciliation_type = 'Cybersource Reconciliation'
);

-- Clean up temporary tables
DROP TABLE IF EXISTS erp_cybersource_sum, credit_card_sum;

COMMIT;

