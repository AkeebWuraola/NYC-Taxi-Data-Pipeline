/* 
===============================================================
DDL Script: Create silver Table Structure
===============================================================
Script Purpose:
This script creates the tables in the silver schema, it drops existing tables if they already exist.
===============================================================
*/

use NYCTaxiTrips

IF OBJECT_ID('silver.yellow_taxi_trip','U') IS NOT NULL
	DROP TABLE silver.yellow_taxi_trip;
	create table silver.yellow_taxi_trip (
	vendor_id int,
	Vendor_name nvarchar(250),
	pickup_time datetime,
	dropoff_time datetime,
	trip_duration_mins int,
	passenger_count int,
	trip_distance float,
	rate_code_id int ,
	rate_code_type nvarchar(50),
	store_and_fwd_flag nvarchar(5),
	pickup_zone int,
	droppoff_zone int,
	payment_type nvarchar(50),
	fare_amount float,
	extra_fees float,
	mta_tax float,
	tip_amount float,
	tolls_amount float,
	improvement_surcharge float,
	total_amount float,
	congestion_surcharge float,	
	airport_fee float
);

