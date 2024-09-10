-- Step 1: Create necessary temporary tables for ECOM Orders Reconciliation

-- Note: Assuming 'ecom_orders' and 'shippedandcollected_aramex_cosmaline' are existing tables and do not need to be created temporarily.

-- Step 2: Insert into reconciliation_results table with duplicate check
INSERT INTO reconciliation_results (reconciliation_type, reference_id, sum_erp_amount_usd, sum_shipping_amount_usd, reconciliation_status, non_existent_record, recon_report)
SELECT 
    'ECOM Orders Reconciliation' AS reconciliation_type,
    e.order_number AS reference_id,
    e.amount AS sum_erp_amount_usd,
    SUM(
        CASE 
            WHEN a.CODCurrency = 'USD' THEN a.O_CODAmount
            WHEN a.CODCurrency = 'LBP' THEN a.CODAmount / COALESCE((SELECT rate FROM daily_rate WHERE date = a.Delivery_Date), 89500)
            ELSE 0
        END
    ) AS sum_cod_amount_usd,
    CASE
        WHEN ABS(e.amount - SUM(
            CASE 
                WHEN a.CODCurrency = 'USD' THEN a.O_CODAmount
                WHEN a.CODCurrency = 'LBP' THEN a.CODAmount / COALESCE((SELECT rate FROM daily_rate WHERE date = a.Delivery_Date), 89500)
                ELSE 0
            END
        )) <= 1 THEN 'Match'
        ELSE 'Mismatch'
    END AS reconciliation_status,
    CASE
        WHEN COUNT(a.order_number) = 0 THEN 1
        ELSE 0
    END AS non_existent_record,
    CASE
        WHEN ABS(e.amount - SUM(
            CASE 
                WHEN a.CODCurrency = 'USD' THEN a.O_CODAmount
                WHEN a.CODCurrency = 'LBP' THEN a.CODAmount / COALESCE((SELECT rate FROM daily_rate WHERE date = a.Delivery_Date), 89500)
                ELSE 0
            END
        )) <= 1 THEN 'Match'
        WHEN COUNT(a.order_number) = 0 THEN 'Mismatch due to non-existent record'
        ELSE 'Mismatch due to different amounts'
    END AS recon_report
FROM 
    ecom_orders e
LEFT JOIN 
    shippedandcollected_aramex_cosmaline a ON e.order_number = a.order_number
WHERE NOT EXISTS (
    SELECT 1 FROM reconciliation_results rr
    WHERE rr.reference_id = e.order_number AND rr.reconciliation_type = 'ECOM Orders Reconciliation'
)
GROUP BY 
    e.order_number;

COMMIT;