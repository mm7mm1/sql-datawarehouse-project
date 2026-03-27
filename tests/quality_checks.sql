/* ============================================================
Check for Nulls or Duplicates in Primary Key
Expectation: No results (Empty set)
============================================================
*/
SELECT 
    cst_id, 
    COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;
/* ============================================================
Check for Unwanted Spaces
Expectation: No results
============================================================
*/
SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

/* ============================================================
Data Standardization & Consistency Check
Purpose: Identify inconsistent values for categorization
============================================================
*/
SELECT DISTINCT 
    prd_line
FROM bronze.crm_prd_info;


/* ============================================================
Check for Nulls or Negative numbers
Expectation: No results (Empty set)
============================================================
*/
SELECT 
    prd_id,
    prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0;
/* ============================================================
Check for invalid order dates
============================================================
*/
SELECT 
    prd_id,
    prd_start_dt,
    prd_end_dt
FROM silver.crm_prd_info
WHERE prd_start_dt > prd_end_dt;