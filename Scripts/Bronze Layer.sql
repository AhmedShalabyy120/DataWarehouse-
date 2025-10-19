-- =============================================
-- Procedure:     bronze.load_bronze
-- Description:   Loads CRM and ERP CSV data into Bronze Layer tables
-- =============================================
CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    DECLARE @start_time DATETIME,
            @end_time   DATETIME;

    SET @start_time = GETDATE();

    BEGIN TRY
        PRINT ' ========================== ';
        PRINT ' Loading Bronze Layer ';
        PRINT ' ========================== ';

        -- =============================================
        -- Load CRM Tables
        -- =============================================
        PRINT ' ------------------------- ';
        PRINT ' Loading CRM Tables ';
        PRINT ' ------------------------- ';

        -- Load CRM Customer Info
        TRUNCATE TABLE bronze.crm_cust_info;

        BULK INSERT bronze.crm_cust_info
        FROM 'G:\SQL Data with Baraa\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        -- Load CRM Product Info
        TRUNCATE TABLE bronze.crm_prod_info;

        BULK INSERT bronze.crm_prod_info
        FROM 'G:\SQL Data with Baraa\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        -- Load CRM Sales Details
        TRUNCATE TABLE bronze.crm_sales_details;

        BULK INSERT bronze.crm_sales_details
        FROM 'G:\SQL Data with Baraa\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        -- =============================================
        -- Load ERP Tables
        -- =============================================
        PRINT ' -------------------------- ';
        PRINT ' Loading ERP Tables ';
        PRINT ' -------------------------- ';

        -- Load ERP Customer Table
        TRUNCATE TABLE bronze.erp_cust_az12;

        BULK INSERT bronze.erp_cust_az12
        FROM 'G:\SQL Data with Baraa\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        -- Load ERP Location Table
        TRUNCATE TABLE bronze.erp_loc_a101;

        BULK INSERT bronze.erp_loc_a101
        FROM 'G:\SQL Data with Baraa\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        -- Load ERP Product Category Table
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'G:\SQL Data with Baraa\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

    END TRY

    BEGIN CATCH
        PRINT ' ===========================';
        PRINT ' ERROR in the Bronze Layer ';
        PRINT ' ERROR MESSAGE: ' + ERROR_MESSAGE();
        PRINT ' ERROR NUMBER : ' + CAST(ERROR_NUMBER() AS VARCHAR);
        PRINT ' ERROR STATE  : ' + CAST(ERROR_STATE() AS VARCHAR);
    END CATCH;

    SET @end_time = GETDATE();

    PRINT ' =====================================================================================================';
    PRINT ' The duration to load the tables is : ' 
          + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR)
          + ' Seconds ';
END;


