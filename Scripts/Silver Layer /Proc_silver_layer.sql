CREATE PROCEDURE etl_load_silver
AS
BEGIN
    SET NOCOUNT ON;

    ---------------------------------------------------------
    -- Declare batch-level and table-level timers
    ---------------------------------------------------------
    DECLARE 
        @batch_start_time DATETIME,
        @batch_end_time   DATETIME,
        @start_time       DATETIME,
        @end_time         DATETIME;

    BEGIN TRY
        
        ---------------------------------------------------------
        -- Start Batch
        ---------------------------------------------------------
        SET @batch_start_time = GETDATE();
        PRINT '=====================================================';
        PRINT '       Starting Silver Layer ETL Batch Load';
        PRINT '=====================================================';
        PRINT 'Batch Start Time: ' + CONVERT(VARCHAR(30), @batch_start_time, 120);
        PRINT '';

        ---------------------------------------------------------
        -- Example: Load CRM Tables Section
        ---------------------------------------------------------
        PRINT '-----------------------------------------------------';
        PRINT '      Loading CRM Tables';
        PRINT '-----------------------------------------------------';
        PRINT '';

        ---------------------------------------------------------
        -- 1️⃣ Load silver.crm_cust_info
        ---------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Loading Table: silver.crm_cust_info';
        PRINT '   Start Time: ' + CONVERT(VARCHAR(30), @start_time, 120);

        TRUNCATE TABLE silver.crm_cust_info;

        INSERT INTO silver.crm_cust_info
        (
            cst_id, cst_key, cst_firstname, cst_lastname,
            cst_material_status, cst_gender, cst_create_date
        )
        SELECT 
            cst_id,
            cst_key,
            TRIM(cst_firstname),
            TRIM(cst_lastname),
            ISNULL(cst_material_status, 'Unknown'),
            cst_gender,
            cst_create_date
        FROM bronze.crm_cust_info;

        SET @end_time = GETDATE();
        PRINT '   End Time:   ' + CONVERT(VARCHAR(30), @end_time, 120);
        PRINT '   Duration:   ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR(10)) + ' seconds';
        PRINT '';

        ---------------------------------------------------------
        -- 2️⃣ Load silver.crm_prod_info
        ---------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Loading Table: silver.crm_prod_info';
        PRINT '   Start Time: ' + CONVERT(VARCHAR(30), @start_time, 120);

        TRUNCATE TABLE silver.crm_prod_info;

        INSERT INTO silver.crm_prod_info
        (
            prod_id, prod_key, category_id, prd_key, 
            prod_num, prod_cost, prod_line, 
            prod_start_dt, prod_end_dt
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
            CAST(LEAD(prod_start_dt) OVER (PARTITION BY prod_key ORDER BY prod_start_dt) - 1 AS DATE)
        FROM bronze.crm_prod_info;

        SET @end_time = GETDATE();
        PRINT '   End Time:   ' + CONVERT(VARCHAR(30), @end_time, 120);
        PRINT '   Duration:   ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR(10)) + ' seconds';
        PRINT '';


        ---------------------------------------------------------
        -- End Batch
        ---------------------------------------------------------
        SET @batch_end_time = GETDATE();

        PRINT '=====================================================';
        PRINT '       Silver Layer ETL Batch Finished';
        PRINT '=====================================================';
        PRINT 'Batch End Time:   ' + CONVERT(VARCHAR(30), @batch_end_time, 120);
        PRINT 'Total Duration:   ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS VARCHAR(15)) + ' seconds';
        PRINT '';

    END TRY
    BEGIN CATCH
        PRINT '❌ ERROR OCCURRED IN SILVER LAYER ETL';
        PRINT ERROR_MESSAGE();
    END CATCH;
END
