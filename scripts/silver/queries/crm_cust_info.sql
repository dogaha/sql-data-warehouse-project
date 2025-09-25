ALTER TABLE silver.crm_cust_info
ALTER COLUMN cst_create_date DATE 

INSERT INTO silver.crm_cust_info (
	cst_id
	,cst_key
	,cst_firstname
	,cst_lastname
	,cst_marital_status
	,cst_gndr
	,cst_create_date)
SELECT
	cst_id
	,cst_key
	,TRIM(cst_firstname) AS cst_firstname
	,TRIM(cst_lastname) AS cst_lastname
	,CASE WHEN TRIM(UPPER(cst_marital_status)) = 'S' THEN 'Single'
		  WHEN TRIM(UPPER(cst_marital_status)) = 'M' THEN 'Married'
		  ELSE 'Unknown'
	END AS cst_marital_status
	,CASE WHEN TRIM(UPPER(cst_gndr)) = 'F' THEN 'Female'
		  WHEN TRIM(UPPER(cst_gndr)) = 'M' THEN 'Male'
		  ELSE 'Unknown'
	END AS cst_gndr
	,CAST(cst_create_date AS DATE) AS cst_create_date
FROM (
	SELECT
	*,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
)t
WHERE flag_last = 1