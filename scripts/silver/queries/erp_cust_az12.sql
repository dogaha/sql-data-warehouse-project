ALTER TABLE silver.erp_cust_az12
ALTER COLUMN bdate DATE


INSERT INTO silver.erp_cust_az12(
	cid,
	bdate,
	gen
)
SELECT
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,len(cid))
	ELSE cid
END AS cid
,CASE WHEN bdate > GETDATE() THEN NULL
	ELSE CAST(bdate AS DATE)
END AS bdate
,CASE WHEN TRIM(UPPER(gen)) IN ('F','FEMALE') THEN 'Female'
	  WHEN TRIM(UPPER(gen)) IN ('M','MALE') THEN 'Male'
	  ELSE 'Unknown'
END AS gen
FROM bronze.erp_cust_az12
