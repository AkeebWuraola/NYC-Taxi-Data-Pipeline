/* 
===============================================================
DDL Script: Create Bronze Table Structure
===============================================================
Script Purpose:
This script creates the tables in the bronze schema, it drops existing tables if they already exist.
===============================================================
*/

USE NYCTaxiTrips

IF OBJECT_ID('bronze.yellow_taxi_trip','U') IS NOT NULL
	DROP TABLE bronze.yellow_taxi_trip;
	create table bronze.yellow_taxi_trip (
	VendorID int,
	tpep_pickup_datetime datetime,
	tpep_dropoff_datetime datetime,
	passenger_count int,
	trip_distance float,
	RatecodeID int ,
	store_and_fwd_flag nvarchar(2),
	PULocationID int,
	DOLocationID int,
	payment_type int,
	fare_amount float,
	extra float,
	mta_tax float,
	tip_amount float,
	tolls_amount float,
	improvement_surcharge float,
	total_amount float,
	congestion_surcharge float,	
	Airport_fee float
);


IF OBJECT_ID('bronze.green_taxi_trip','U') IS NOT NULL
	DROP TABLE bronze.green_taxi_trip;
	create table bronze.green_taxi_trip (
	VendorID int,
	lpep_pickup_datetime datetime,
	lpep_dropoff_datetime datetime,
	store_and_fwd_flag nvarchar(2),
	RatecodeID int ,
	PULocationID int,
	DOLocationID int,
	passenger_count int,
	trip_distance float,
	fare_amount float,
	extra float,
	mta_tax float,
	tip_amount float,
	tolls_amount float,
	ehail_fee float,
	improvement_surcharge float,
	total_amount float,
	payment_type int,
	trip_type int,
	congestion_surcharge float
);

