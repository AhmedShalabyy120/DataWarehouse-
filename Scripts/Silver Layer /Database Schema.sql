-- ===============================================
-- Create Silver Layer Tables for Refined Data
-- ===============================================

-- =====================================================
-- Customer Information (from CRM source)
-- =====================================================
IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_cust_info;

CREATE TABLE silver.crm_cust_info
(
    cst_id              INT,             -- Customer ID (numeric identifier)
    cst_key             VARCHAR(50),     -- Unique key from CRM source
    cst_firstname       VARCHAR(50),     -- Customer first name
    cst_lastname        VARCHAR(50),     -- Customer last name
    cst_material_status VARCHAR(50),     -- Marital status of customer
    cst_gender          VARCHAR(50),     -- Gender (Male/Female)
    cst_create_date     DATE,            -- Date customer record was created
    dwh_create_date     DATETIME2 DEFAULT GETDATE() -- Insertion date in the DWH
);


-- =====================================================
-- Product Information (from CRM source)
-- =====================================================
IF OBJECT_ID('silver.crm_prod_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_prod_info;

CREATE TABLE silver.crm_prod_info
(
    prod_id         INT,             -- Product ID (numeric identifier)
    prod_key        VARCHAR(50),     -- Unique product key from CRM
    category_id     VARCHAR(50),     
    prd_key         VARCHAR(50),
    prod_num        VARCHAR(50),     -- Product number or SKU
    prod_cost       INT,             -- Product cost
    prod_line       VARCHAR(50),     -- Product line (category group)
    prod_start_dt   DATE,            -- Product availability start date
    prod_end_dt     DATE,            -- Product availability end date
    dwh_create_date DATETIME2 DEFAULT GETDATE() -- Insertion date in the DWH
);


-- =====================================================
-- Sales Details (from CRM source)
-- =====================================================
IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE silver.crm_sales_details;

CREATE TABLE silver.crm_sales_details
(
    sls_order_numb  VARCHAR(50),     -- Sales order number
    sls_prod_key    VARCHAR(50),     -- Product key (foreign key to product info)
    sls_cust_id     INT,             -- Customer ID (foreign key to customer info)
    sls_order_dt    INT,             -- Order date (to be converted to proper date)
    sls_ship_dt     INT,             -- Shipment date (to be converted to proper date)
    sls_due_dt      INT,             -- Due date (to be converted to proper date)
    sls_sales       INT,             -- Sales amount
    sls_quantity    INT,             -- Quantity sold
    sls_price       INT,             -- Unit price
    dwh_create_date DATETIME2 DEFAULT GETDATE() -- Insertion date in the DWH
);


-- =====================================================
-- ERP Customer Information (from ERP system)
-- =====================================================
IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE silver.erp_cust_az12;

CREATE TABLE silver.erp_cust_az12
(
    cid             VARCHAR(50),    -- Customer ID from ERP system
    bdate           DATE,           -- Birth date of the customer
    gen             VARCHAR(50),    -- Gender information
    dwh_create_date DATETIME2 DEFAULT GETDATE() -- Insertion date in the DWH
);


-- =====================================================
-- ERP Product Category Information
-- =====================================================
IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE silver.erp_px_cat_g1v2;

CREATE TABLE silver.erp_px_cat_g1v2
(
    id              VARCHAR(50),    -- Product ID or key
    cat             VARCHAR(50),    -- Product category
    subcat          VARCHAR(50),    -- Product subcategory
    maintenance     VARCHAR(50),    -- Maintenance type or level
    dwh_create_date DATETIME2 DEFAULT GETDATE() -- Insertion date in the DWH
);


-- =====================================================
-- ERP Location Information
-- =====================================================
IF OBJECT_ID('silver.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE silver.erp_loc_a101;

CREATE TABLE silver.erp_loc_a101
(
    cid             VARCHAR(50),    -- Customer ID reference
    cntry           VARCHAR(50),    -- Country name or code
    dwh_create_date DATETIME2 DEFAULT GETDATE() -- Insertion date in the DWH
);
