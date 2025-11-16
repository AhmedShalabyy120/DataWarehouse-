-- ============================================
-- Create Gold Dimension Table: product_info
-- Maps product details from CRM and enriches with category info from ERP
-- ============================================
CREATE VIEW gold.product_info AS
SELECT 
    -- Surrogate Key
    ROW_NUMBER() OVER (ORDER BY pn.prod_id) AS product_key,  -- Unique sequential key for product dimension

    -- Source Product Identifiers
    pn.prod_id          AS product_id,        -- Original product ID from CRM
    pn.Category_id      AS Category_id,       -- Category ID from CRM
    pn.prd_key          AS product_number,    -- Product key in source system
    pn.prod_num         AS product_name,      -- Product name

    -- Measures / Attributes
    pn.prod_cost        AS product_cost,      -- Cost of product
    pc.maintenance      AS maintenance,       -- Maintenance info from ERP
    pc.cat              AS category,          -- Category name from ERP
    pc.subcat           AS subcategory,       -- Subcategory name from ERP
    pn.prod_line        AS product_line,      -- Product line
    pn.prod_start_dt    AS start_date         -- Product start date

FROM silver.crm_prod_info AS pn

-- Join to ERP category table to enrich product info
LEFT JOIN silver.erp_px_cat_g1v2 AS pc
    ON pc.id = pn.Category_id

-- Exclude historical/ended products
WHERE pn.prod_end_dt IS NULL;
