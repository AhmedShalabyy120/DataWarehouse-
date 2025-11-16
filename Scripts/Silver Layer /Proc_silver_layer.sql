CREATE OR ALTER PROCEDURE silver.load_silver 
AS
BEGIN


    --------------------------------------------
    -- Declare timers
    --------------------------------------------
    DECLARE 
        @batch_start_time DATETIME,
        @batch_end_time   DATETIME,
        @start_time       DATETIME,
        @end_time         DATETIME;

    --------------------------------------------
    -- Start batch
    --------------------------------------------
    SET @batch_start_time = GETDATE();
    PRINT '=====================================================';
    PRINT '        Starting Silver Layer Load';
    PRINT '=====================================================';
    PRINT 'Batch Start Time: ' + CONVERT(VARCHAR(30), @batch_start_time, 120);
    PRINT '';

    /* ============================================================
                       CRM TABLES
       ============================================================ */

    ---------------------------------------------------------------
    -- CRM: crm_prod_info
    ---------------------------------------------------------------
    SET @start_time = GETDATE();
    PRINT '>> Loading silver.crm_prod_info (Start: ' + CONVERT(VARCHAR(30), @start_time, 120) + ')';

    TRUNCATE TABLE silver.crm_prod_info;

    INSERT INTO silver.crm_prod_info
    (
        prod_id,
        prod_key,
        Category_id,
        prd_key,
        prod_num,
        prod_cost,
        prod_line,
        prod_start_dt,
        prod_end_dt
    )
    SELECT 
        prod_id,
        prod_key,
        REPLACE(SUBSTRING(prod_key, 1, 5), '-', '_'),
        SUBSTRING(prod_key, 7, LEN(prod_key)),
        prod_num,
        ISNULL(prod_cost, 0),
        CASE 
            WHEN UPPER(TRIM(prod_line)) = 'T' THEN 'Touring'
            WHEN UPPER(TRIM(prod_line)) = 'S' THEN 'Other Sales'
            WHEN UPPER(TRIM(prod_line)) = 'R' THEN 'Road'
            WHEN UPPER(TRIM(prod_line)) = 'M' THEN 'Mountain'
            ELSE 'Unknown'
        END,
        CAST(prod_start_dt AS DATE),
        CAST(LEAD(prod_start_dt) OVER (PARTITION BY prod_key ORDER BY prod_start_dt ASC) - 1 AS DATE)
    FROM bronze.crm_prod_info;

    SET @end_time = GETDATE();
    PRINT '   Completed in ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR(10)) + ' sec';
    PRINT '';


    ---------------------------------------------------------------
    -- CRM: crm_sales_details
    ---------------------------------------------------------------
    SET @start_time = GETDATE();
    PRINT '>> Loading silver.crm_sales_details (Start: ' + CONVERT(VARCHAR(30), @start_time, 120) + ')';

    TRUNCATE TABLE silver.crm_sales_details;

    INSERT INTO silver.crm_sales_details 
    (
        sls_order_numb,
        sls_prod_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_price,
        sls_quantity
    )
    SELECT
        sls_order_numb,
        sls_prod_key,
        sls_cust_id,

        CASE 
            WHEN sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 THEN NULL
            ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
        END,

        CASE 
            WHEN sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8 THEN NULL
            ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
        END,

        CASE 
            WHEN sls_due_dt <= 0 OR LEN(sls_due_dt) != 8 THEN NULL
            ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
        END,

        CASE 
            WHEN sls_sales IS NULL 
              OR sls_sales < 0 
              OR sls_sales != sls_quantity * ABS(sls_price)
            THEN sls_quantity * ABS(sls_price)
            ELSE sls_sales
        END,

        CASE 
            WHEN sls_price IS NULL OR sls_price < 0 THEN sls_sales / NULLIF(sls_quantity, 0)
            ELSE sls_price
        END,

        sls_quantity
    FROM bronze.crm_sales_details;

    SET @end_time = GETDATE();
    PRINT '   Completed in ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR(10)) + ' sec';
    PRINT '';


    ---------------------------------------------------------------
    -- CRM: crm_cust_info
    ---------------------------------------------------------------
    SET @start_time = GETDATE();
    PRINT '>> Loading silver.crm_cust_info (Start: ' + CONVERT(VARCHAR(30), @start_time, 120) + ')';

    TRUNCATE TABLE silver.crm_cust_info;

    INSERT INTO silver.crm_cust_info 
    ( 
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname, 
        cst_material_status,
        cst_gender,
        cst_create_date
    )
    SELECT 
        cst_id,
        cst_key,
        TRIM(cst_firstname),
        TRIM(cst_lastname),
        CASE WHEN cst_material_status IS NULL THEN 'Unknown' ELSE cst_material_status END,
        CASE WHEN cst_gender IS NULL THEN 'Unknown' ELSE cst_gender END,
        cst_create_date
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date) AS Unique_PK
        FROM bronze.crm_cust_info
        WHERE cst_id IS NOT NULL
    ) t
    WHERE Unique_PK = 1;

    SET @end_time = GETDATE();
    PRINT '   Completed in ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR(10)) + ' sec';
    PRINT '';



    /* ============================================================
                       ERP TABLES
       ============================================================ */

    ---------------------------------------------------------------
    -- ERP: erp_cust_az12
    ---------------------------------------------------------------
    SET @start_time = GETDATE();
    PRINT '>> Loading silver.erp_cust_az12 (Start: ' + CONVERT(VARCHAR(30), @start_time, 120) + ')';

    TRUNCATE TABLE silver.erp_cust_az12;

    INSERT INTO silver.erp_cust_az12 
    (
        cid,
        bdate,
        gen
    )
    SELECT 
        CASE 
            WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
            ELSE cid
        END,

        CASE 
            WHEN bdate >= GETDATE() OR bdate <= '1950-01-01' THEN NULL
            ELSE bdate
        END,

        CASE 
            WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'M'
            WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'F'
            ELSE 'Unknown'
        END
    FROM bronze.erp_cust_az12;

    SET @end_time = GETDATE();
    PRINT '   Completed in ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR(10)) + ' sec';
    PRINT '';


    ---------------------------------------------------------------
    -- ERP: erp_loc_a101
    ---------------------------------------------------------------
    SET @start_time = GETDATE();
    PRINT '>> Loading silver.erp_loc_a101 (Start: ' + CONVERT(VARCHAR(30), @start_time, 120) + ')';

    TRUNCATE TABLE silver.erp_loc_a101;

    INSERT INTO silver.erp_loc_a101
    (
        cid,
        cntry
    )
    SELECT 
        REPLACE(cid, '-', ''),
        CASE 
            WHEN TRIM(cntry) = 'DE' THEN 'Germany'
            WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
            WHEN cntry IS NULL OR cntry = '' THEN 'Unknown'
            ELSE TRIM(cntry)
        END
    FROM bronze.erp_loc_a101;

    SET @end_time = GETDATE();
    PRINT '   Completed in ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR(10)) + ' sec';
    PRINT '';


    ---------------------------------------------------------------
    -- ERP: erp_px_cat_g1v2
    ---------------------------------------------------------------
    SET @start_time = GETDATE();
    PRINT '>> Loading silver.erp_px_cat_g1v2 (Start: ' + CONVERT(VARCHAR(30), @start_time, 120) + ')';

    TRUNCATE TABLE silver.erp_px_cat_g1v2;

    INSERT INTO silver.erp_px_cat_g1v2 
    (
        id,
        cat,
        subcat,
        maintenance
    )
    SELECT 
        id,
        TRIM(cat),
        TRIM(subcat),
        TRIM(maintenance)
    FROM bronze.erp_px_cat_g1v2;

    SET @end_time = GETDATE();
    PRINT '   Completed in ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR(10)) + ' sec';
    PRINT '';

    --------------------------------------------
    -- End batch
    --------------------------------------------
    SET @batch_end_time = GETDATE();

    PRINT '=====================================================';
    PRINT '        Silver Layer Load Completed';
    PRINT '=====================================================';
    PRINT 'Batch End Time: ' + CONVERT(VARCHAR(30), @batch_end_time, 120);
    PRINT 'Total Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS VARCHAR(15)) + ' sec';
    PRINT '';
END;




