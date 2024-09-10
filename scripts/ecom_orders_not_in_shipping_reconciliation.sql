-- Step 1: Create necessary temporary tables for ECOM Orders not in Shipping Reconciliation

-- Note: Assuming 'ecom_orders' and 'shippedandcollected_aramex_cosmaline' are existing tables and do not need to be created temporarily.

-- Step 2: Insert into reconciliation_results table with duplicate check
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
    a.order_number IS NULL
AND NOT EXISTS (
    SELECT 1 FROM reconciliation_results rr
    WHERE rr.reference_id = e.order_number AND rr.reconciliation_type = 'ECOM Orders not in Shipping'
);

COMMIT;
