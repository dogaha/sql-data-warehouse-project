--Check for Null's or Duplicates in primary key
--Expected: no result
Select
prd_id,
COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

--Check for unwanted spaces
--Expectation: no results
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

--Check for Nulls or Negative Numbers
--Expectation: no results
Select prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

--Check Consistency
SELECT DISTINCT gen
FROM bronze.erp_cust_az12

--Check for invalid date orders
--Where start date is after end date
SELECT * FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

--Final look
SELECT * FROM silver.erp_loc_a101


--Check for invalid dates 
--Dates should not be zero or negative
SELECT 
sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 
OR LEN(sls_order_dt) != 8
--or ouside organization boundaries 

--check business rules
SELECT DISTINCT
       sls_sales as sls_old_sales
      ,sls_quantity
      ,sls_price as sls_old_price
      ,CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != ABS(sls_price) * sls_quantity THEN ABS(sls_price) * sls_quantity
            ELSE sls_sales
        END AS sls_sales
      ,CASE WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity,0)
            ELSE sls_price
        END AS sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL 
OR sls_quantity <= 0 OR sls_price <= 0 OR sls_sales <= 0
ORDER BY sls_sales, sls_quantity, sls_price

SELECT * FROM silver.crm_prd_info

--Check unwanted spaces
SELECT * FROM bronze.erp_px_cat_g1v2
WHERE maintenance != TRIM(maintenance)

--Data Standardization adn Consistancy
SELECT DISTINCT
maintenance
FROM bronze.erp_px_cat_g1v2