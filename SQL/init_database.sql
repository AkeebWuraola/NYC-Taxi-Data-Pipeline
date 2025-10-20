/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'NYCTaxiTrips' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
	
WARNING:
    Running this script will drop the entire 'NYCTaxiTrips' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/

USE master;
GO

---Drop and recreate the database
IF EXISTS (SELECT * FROM sys.databases WHERE name = 'NYCTaxiTrips')
BEGIN 
	ALTER DATABASE NYCTaxiTrips SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE NYCTaxiTrips;
END
GO

-- Create Database 'NYCTaxiTrips'
CREATE DATABASE NYCTaxiTrips;
GO

USE NYCTaxiTrips;
GO

--Create Schemas
/*A schema is like a folder or container that helps to keep things organized*/

CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
