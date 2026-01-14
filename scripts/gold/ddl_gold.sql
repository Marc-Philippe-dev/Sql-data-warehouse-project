/*
---------------------------------------------------------------------------------------
 File Name   :  ddl_gold.sql
 Date        :  2026-14-01
 Author      : GNANCADJA Marc Philippe 
 Description : Creation of Gold Layer Dimension and Fact Views for the Data Warehouse.

 This script creates three views in the `gold` schema:
   1. dim_customers  – Customer dimension with surrogate keys and enriched attributes
   2. dim_products   – Product dimension with category mappings and active products only
   3. fact_sales     – Sales fact table integrating customer & product surrogate keys

 Purpose:
   These views form the semantic “Gold” layer of the data warehouse. 
   They expose cleaned, conformed, and business-ready datasets derived from the Silver layer.

 General Business Logic:
   - Surrogate Keys:
        Each dimension view generates a surrogate key using ROW_NUMBER(), ordered by 
        the natural identifier. These keys replace business identifiers in fact tables.
   
   - Data Conformance:
        The views standardize attributes (e.g., gender, category, country) and
        resolve inconsistencies by joining CRM and ERP sources.

   - Dimension Views:
        dim_customers:
            - Combines CRM and ERP customer data
            - Resolves gender inconsistencies
            - Adds country information
            - Generates surrogate key `customer_key`

        dim_products:
            - Joins product metadata with category/subcategory information
            - Filters inactive products (prd_end_dt IS NULL)
            - Generates surrogate key `product_key`

   - Fact View:
        fact_sales:
            - Uses natural keys from the silver sales details table
            - Joins with dimensions to retrieve surrogate keys
            - Filters out invalid dimension records naturally via the joins

 Maintenance:
   Running this script is idempotent thanks to:
        DROP VIEW IF EXISTS ...
   This ensures that views are recreated cleanly without pre-checks.

---------------------------------------------------------------------------------------
*/

-- Customer Dimension View
DROP VIEW IF EXISTS gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
SELECT 
	ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key,
	ci.cst_id as customer_id, 
	ci.cst_key as customer_number, 
	ci.cst_firstname as first_name,
	ci.cst_lastname as last_name,
	la.cntry as country,
	ci.cst_marital_status as marital_status,	
	ci.cst_created_date as created_date,
	ca.bdate as birthdate,
	CASE 
		WHEN ci.cst_gender !='n/a' THEN ci.cst_gender
		ELSE coalesce(ca.gen, 'n/a')	
	END as gender
	
FROM silver.crm_cust_info ci 
LEFT  JOIN silver.erp_cust_az12 ca 
ON ci.cst_key  = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid;

GO
-- Product Dimension View
DROP VIEW IF EXISTS gold.dim_products;
GO

CREATE  VIEW gold.dim_products AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY pn.prd_id) AS product_key,
	pn.prd_id as product_id,
	pn.prd_key as product_number,
	pn.prd_nm as product_name, 
	pn.cat_id as category_id,
	pc.cat as category,
	pc.subcat as subcategory,
	pc.maintenance ,
	pn.prd_cost as cost,
	pn.prd_line as product_line, 
	pn.prd_start_dt as start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc 
ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL;
GO

-- Create a view for fact table sales
DROP VIEW IF EXISTS gold.fact_sales;
GO
CREATE  VIEW gold.fact_sales AS

SELECT
	sd.sls_ord_num as order_number,
	pr.product_key ,
	cu.customer_key ,
	sd.sls_order_dt as order_date,
	sd.sls_ship_dt as shipping_date,
	sd.sls_due_dt as due_date,
	sd.sls_sales as sales_amount,
	sd.sls_quantity as quantity_sold,
	sd.sls_price as sales_price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr 
ON sd.sls_prd_key = pr.product_number -- to ensure only valid products are considered and only keep surrogate key coming from the product dimension
LEFT JOIN gold.dim_customers cu		
ON sd.sls_cust_id = cu.customer_id; -- to ensure only valid customers are considered and only keep surrogate key coming from the customer dimension