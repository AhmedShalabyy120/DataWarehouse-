-- Create a new database to serve as the Data Warehouse
create database DataWareHouse;

-- Switch context to the newly created DataWareHouse database
use DataWareHouse;
go

-- Create the 'bronze' schema 
-- This layer stores raw, unprocessed data directly from source systems
create schema bronze;
go

-- Create the 'silver' schema 
-- This layer stores cleaned and transformed data after processing
create schema silver;
go

-- Create the 'gold' schema 
-- This layer stores aggregated, business-ready data for reporting and analytics
create schema gold;
go

-- ===============================================
-- Create Bronze Layer Tables for Raw Data Ingestion
-- ===============================================

-- =====================================================
-- Customer Information (from CRM source)
-- =====================================================
CREATE TABLE bronze.crm_cust_info
(
    cst_id              INT,             -- Customer ID (numeric identifier)
    cst_key             VARCHAR(50),     -- Unique key from CRM source
    cst_firstname       VARCHAR(50),     -- Customer first name
    cst_lastname        VARCHAR(50),     -- Customer last name
    cst_material_status VARCHAR(50),     -- Marital status of customer
    cst_gender          VARCHAR(50),     -- Gender (Male/Female)
    cst_create_date     DATE             -- Date customer record was created
);


-- =====================================================
-- Product Information (from CRM source)
-- =====================================================
CREATE TABLE bronze.crm_prod_info
(
    prod_id       INT,              -- Product ID (numeric identifier)
    prod_key      VARCHAR(50),      -- Unique product key from CRM
    prod_num      VARCHAR(50),      -- Product number or SKU
    prod_cost     INT,              -- Product cost
    prod_line     VARCHAR(50),      -- Product line (category group)
    prod_start_dt DATETIME,         -- Product availability start date
    prod_end_dt   DATETIME          -- Product availability end date
);


-- =====================================================
-- Sales Details (from CRM source)
-- =====================================================
CREATE TABLE bronze.crm_sales_details
(
    sls_order_numb  VARCHAR(50),    -- Sales order number
    sls_prod_key    VARCHAR(50),    -- Product key (foreign key to product info)
    sls_cust_id     INT,            -- Customer ID (foreign key to customer info)
    sls_order_dt    INT,            -- Order date (to be converted to proper date)
    sls_ship_dt     INT,            -- Shipment date (to be converted to proper date)
    sls_due_dt      INT,            -- Due date (to be converted to proper date)
    sls_sales       INT,            -- Sales amount
    sls_quantity    INT,            -- Quantity sold
    sls_price       INT             -- Unit price
);


-- =====================================================
-- ERP Customer Information (from ERP system)
-- =====================================================
CREATE TABLE bronze.erp_cust_az12
(
    cid    VARCHAR(50),             -- Customer ID from ERP system
    bdate  DATE,                    -- Birth date of the customer
    gen    VARCHAR(50)              -- Gender information
);


-- =====================================================
-- ERP Product Category Information
-- =====================================================
CREATE TABLE bronze.erp_px_cat_g1v2
(
    id           VARCHAR(50),       -- Product ID or key
    cat          VARCHAR(50),       -- Product category
    subcat       VARCHAR(50),       -- Product subcategory
    maintenance  VARCHAR(50)        -- Maintenance type or level
);


-- =====================================================
-- ERP Location Information
-- =====================================================
CREATE TABLE bronze.erp_loc_a101
(
    cid    VARCHAR(50),             -- Customer ID reference
    cntry  VARCHAR(50)              -- Country name or code
);

