-- ============================================
-- Create Gold Fact Table: fact_sales
-- Links sales transactions with product & customer dimensions
-- ============================================
CREATE VIEW gold.fact_sales AS
SELECT 
    -- Dimension Keys
    pr.product_key,          -- Surrogate key from Product Dimension
    cst.customer_key,        -- Surrogate key from Customer Dimension
    
    -- Source Identifiers
    sd.sls_order_numb     AS order_number,     -- Original order number from CRM
    sd.sls_prod_key       AS source_prod_key,  -- Product key from source system
    sd.sls_cust_id        AS source_cst_key,   -- Customer key from source system

    -- Dates
    sd.sls_order_dt       AS order_date,       -- Date order was placed
    sd.sls_ship_dt        AS ship_date,        -- Date order was shipped
    sd.sls_due_dt         AS due_date,         -- Expected delivery date

    -- Measures
    sd.sls_sales          AS sales,            -- Total sales amount
    sd.sls_quantity       AS quantity,         -- Units sold
    sd.sls_price          AS price             -- Unit price
FROM silver.crm_sales_details AS sd

-- Join to product dimension using product number
LEFT JOIN gold.product_info AS pr 
    ON sd.sls_prod_key = pr.product_number

-- Join to customer dimension using customer ID
LEFT JOIN gold.customer_info AS cst
    ON sd.sls_cust_id = cst.customer_id;
