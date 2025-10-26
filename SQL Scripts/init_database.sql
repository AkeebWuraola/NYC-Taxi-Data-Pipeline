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

USE NYCTaxiTrips;
GO
----------Create a log table for tracking process and pipeline load	
IF OBJECT_ID('log_process_status','U') IS NOT NULL
DROP TABLE log_process_status;
CREATE TABLE  log_process_status (
	id int identity(1,1) primary key,
	process_type nvarchar(500),
	process_name nvarchar(500),
	table_name nvarchar(500),
	status nvarchar(50),
	row_count int,
	process_start_time datetime,
	process_end_time datetime,
	error_number int null,
	error_message nvarchar(max) null,
	error_line int null,
	error_procedure nvarchar(max) null,
	load_date datetime default getdate() 
);
GO

----------Create a file tracking (metadata) table
create table bronze.etl_file_log (
    file_name nvarchar(255) primary key,
    table_name nvarchar(255),
    load_timestamp datetime default getdate(),
    row_count int,
    load_status varchar(20) default 'SUCCESS'
);
