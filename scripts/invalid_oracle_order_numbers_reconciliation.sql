-- Step 1: Create necessary temporary tables for Invalid Oracle Order Numbers Reconciliation

-- Note: Assuming 'oracle_data' and 'ecom_orders' are existing tables and do not need to be created temporarily.

-- Step 2: Insert into reconciliation_results table with duplicate check
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
    e.order_number IS NULL AND o.ecom_reference_order_number IS NOT NULL
AND NOT EXISTS (
    SELECT 1 FROM reconciliation_results rr
    WHERE rr.reference_id = o.ecom_reference_order_number AND rr.reconciliation_type = 'Invalid Oracle Order Numbers'
);

COMMIT;
