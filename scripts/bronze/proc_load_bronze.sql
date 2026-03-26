/*
===============================================================================
Stored Procedure: Load Bronze Layer (Raw Data Ingestion)
===============================================================================
Description:
    This procedure truncates and reloads raw CSV data into the 'bronze' schema.
    It includes error handling to ensure data integrity and easier debugging.
    
Usage:
    CALL bronze.load_bronze();
===============================================================================
*/


CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP;
    v_end_time   TIMESTAMP;
    v_procedure_start_time TIMESTAMP;
    v_procedure_end_time TIMESTAMP;
BEGIN
    v_procedure_start_time := clock_timestamp();

    -- Start of the execution block with error handling
    BEGIN
        RAISE NOTICE '==================================================';
        RAISE NOTICE '   STARTING BRONZE LAYER LOAD PROCESS';
        RAISE NOTICE '==================================================';

        -- 1. Loading CRM Tables
        RAISE NOTICE '--------------------------------------------------';
        RAISE NOTICE '>> Ingesting CRM System Data...';
        RAISE NOTICE '--------------------------------------------------';

        -- Table 1: crm_cust_info
        v_start_time := clock_timestamp();
        RAISE NOTICE '   -> Loading bronze.crm_cust_info.csv';
        TRUNCATE TABLE bronze.crm_cust_info;
        COPY bronze.crm_cust_info FROM '/datasets/source_crm/cust_info.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',');
        v_end_time := clock_timestamp();
        RAISE NOTICE '   ++ Success. Load time: % seconds', EXTRACT(EPOCH FROM (v_end_time - v_start_time));

        -- Table 2: crm_prd_info
        v_start_time := clock_timestamp();
        RAISE NOTICE '   -> Loading bronze.crm_prd_info.csv';
        TRUNCATE TABLE bronze.crm_prd_info;
        COPY bronze.crm_prd_info FROM '/datasets/source_crm/prd_info.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',');
        v_end_time := clock_timestamp();
        RAISE NOTICE '   ++ Success. Load time: % seconds', EXTRACT(EPOCH FROM (v_end_time - v_start_time));

        -- Table 3: crm_sales_details
        v_start_time := clock_timestamp();
        RAISE NOTICE '   -> Loading bronze.crm_sales_details.csv';
        TRUNCATE TABLE bronze.crm_sales_details;
        COPY bronze.crm_sales_details FROM '/datasets/source_crm/sales_details.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',');
        v_end_time := clock_timestamp();
        RAISE NOTICE '   ++ Success. Load time: % seconds', EXTRACT(EPOCH FROM (v_end_time - v_start_time));

        -- 2. Loading ERP Tables
        RAISE NOTICE '--------------------------------------------------';
        RAISE NOTICE '>> Ingesting ERP System Data...';
        RAISE NOTICE '--------------------------------------------------';

        -- Table 4: erp_loc_a101
        v_start_time := clock_timestamp();
        RAISE NOTICE '   -> Loading bronze.erp_loc_a101.csv';
        TRUNCATE TABLE bronze.erp_loc_a101;
        COPY bronze.erp_loc_a101 FROM '/datasets/source_erp/loc_a101.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',');
        v_end_time := clock_timestamp();
        RAISE NOTICE '   ++ Success. Load time: % seconds', EXTRACT(EPOCH FROM (v_end_time - v_start_time));

        -- Table 5: erp_cust_az12
        v_start_time := clock_timestamp();
        RAISE NOTICE '   -> Loading bronze.erp_cust_az12.csv';
        TRUNCATE TABLE bronze.erp_cust_az12;
        COPY bronze.erp_cust_az12 FROM '/datasets/source_erp/cust_az12.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',');
        v_end_time := clock_timestamp();
        RAISE NOTICE '   ++ Success. Load time: % seconds', EXTRACT(EPOCH FROM (v_end_time - v_start_time));

        -- Table 6: erp_px_cat_g1v2
        v_start_time := clock_timestamp();
        RAISE NOTICE '   -> Loading bronze.erp_px_cat_g1v2.csv';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        COPY bronze.erp_px_cat_g1v2 FROM '/datasets/source_erp/px_cat_g1v2.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',');
        v_end_time := clock_timestamp();
        RAISE NOTICE '   ++ Success. Load time: % seconds', EXTRACT(EPOCH FROM (v_end_time - v_start_time));

        RAISE NOTICE '==================================================';
        RAISE NOTICE '   SUCCESS: BRONZE LAYER LOADED SUCCESSFULLY';
        RAISE NOTICE '==================================================';
        v_procedure_end_time := clock_timestamp();
        RAISE NOTICE '   TOTAL Procedure Execution Time: % seconds', EXTRACT(EPOCH FROM (v_procedure_end_time - v_procedure_start_time));

    -- EXCEPTION block
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE '==================================================';
        RAISE NOTICE '   ERROR: An unexpected error occurred!';
        RAISE NOTICE '   Error Message: %', SQLERRM;
        RAISE NOTICE '==================================================';
    END;
END;
$$;