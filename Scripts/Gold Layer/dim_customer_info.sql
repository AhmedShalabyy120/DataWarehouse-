-- ============================================
-- Create Gold Dimension Table: customer_info
-- Maps customer details from CRM and enriches with ERP demographic & location info
-- ============================================
CREATE VIEW gold.customer_info AS
SELECT 
    -- Surrogate Key
    ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key,   -- Unique sequential key for customer dimension

    -- Source Customer Identifiers
    ci.cst_id               AS customer_id,       -- Original customer ID from CRM
    ci.cst_key              AS customer_number,   -- Customer key in source system
    ci.cst_firstname        AS first_name,        -- First name
    ci.cst_lastname         AS last_name,         -- Last name

    -- Demographics
    CASE 
        WHEN ci.cst_gender != 'Unknown' THEN ci.cst_gender
        ELSE COALESCE(ca.gen, 'Unknown')
    END                     AS gender,            -- Gender, using ERP data if CRM unknown
    la.cntry                 AS country,          -- Country from ERP location table
    ci.cst_material_status   AS marital_status,   -- Marital status

    -- Dates
    ci.cst_create_date       AS create_date,      -- Customer creation date
    ca.bdate                 AS birth_date        -- Birth date from ERP

FROM silver.crm_cust_info AS ci

-- Join to ERP demographic table for additional info
LEFT JOIN silver.erp_cust_az12 AS ca
    ON ci.cst_key = ca.cid

-- Join to ERP location table for country info
LEFT JOIN silver.erp_loc_a101 AS la
    ON ci.cst_key = la.cid;
