select * from bronze.yellow_taxi_trip
select * from [bronze].[green_taxi_trip]
select * from bronze.etl_file_log
select min(tpep_pickup_datetime),max(tpep_pickup_datetime) from bronze.yellow_taxi_trip

select * from bronze.yellow_taxi_trip where cast(tpep_pickup_datetime as date) not between '2024-01-01' and '2024-12-31'
--use NYCTaxiTrips

truncate table bronze.yellow_taxi_trip
truncate table bronze.etl_file_log
--truncate table log_process_status

select * from log_process_status
--select * from bronze.etl_file_log
select count(*) from bronze.yellow_taxi_trip

implement a re run in case a file was changed---a case where by the table only delete the month you want to reload.
