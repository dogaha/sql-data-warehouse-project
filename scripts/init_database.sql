/*
Create Database and Schemas

Purpose:
	This script will create a new database named 'DataWarehouse'
	it will automatically check if it exists and if it does will delete it
	It will also add three schemas 'bronze','silver', and 'gold'

Warning:
	This script wil automatically delete any databases named 'DataWarehouse' 
*/

--create database 'DataWarehouse'
USE master;
GO
--Check if database exists
IF EXISTS(SELECT * FROM sys.databases WHERE name ='DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END
GO

CREATE DATABASE DataWarehouse
GO

USE DataWarehouse
GO

CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
