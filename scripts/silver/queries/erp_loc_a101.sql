INSERT INTO silver.erp_loc_a101(
	cid,
	cntry
)
SELECT
REPLACE(cid,'-','') AS cid
,CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	  WHEN Trim(cntry) IN ('USA','US') Then 'United States'
	  WHEN TRIM(cntry) IS NULL OR TRIM(cntry) = '' THEN 'Unknown'
	  ELSE cntry
END AS cntry
FROM bronze.erp_loc_a101;