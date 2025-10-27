/*
=====================================================================================
Sript Purpose:
This stored procedure loads data into the silver schema from staging tables in the bronze schema
It truncates the silver tables before loading the data
It inserts data that has been transformed and cleaned from the bronze schema into the silver tables.
=====================================================================================
*/
insert into silver.yellow_taxi_trip(vendor_id, vendor_name , pickup_time, dropoff_time , trip_duration_mins, passenger_count, trip_distance 
	, rate_code_id , rate_code_type ,	store_and_fwd_flag,	pickup_zone, droppoff_zone, payment_type, fare_amount, extra_fees
	, mta_tax, tip_amount,	tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, airport_fee)
select vendorID vendor_id,
case 
	when vendorID = 1 then 'Creative Mobile Technologies, LLC'
	when vendorID = 2 then 'Curb Mobility, LLC'
	when vendorID = 6 then 'Myle Technologies Inc'
	when vendorID = 7 then 'Helix'
	else 'No Vendor'
end Vendor_name
,tpep_pickup_datetime pickup_time,tpep_dropoff_datetime dropoff_time,datediff(minute,tpep_pickup_datetime,tpep_dropoff_datetime)trip_duration_mins
,passenger_count, trip_distance,ratecodeid rate_code_id
,case 
	when ratecodeid = 1 then 'Standard rate'
	when ratecodeid = 2 then 'JFK'
	when ratecodeid = 3 then 'Newark'
	when ratecodeid = 4 then 'Nassau or Westchester'
	when ratecodeid = 5 then 'Negotiated fare'
	when ratecodeid = 6 then 'Group ride'
	when ratecodeid = 99 then 'Unknown'
	else 'N/A'
end rate_code_type
,isnull(replace(store_and_fwd_flag,'"',''),'N/A')store_and_forward_trip_flag, PULocationID pickup_zone,DOLocationID droppoff_zone
,case
	when payment_type = 0 then 'Flex Fare trip'
	when payment_type = 1 then 'Credit card'
	when payment_type = 2 then 'Cash'
	when payment_type = 3 then 'No charge'
	when payment_type = 4 then 'Dispute'
	when payment_type = 5 then 'Unknown'
	when payment_type = 6 then 'Voided trip'
	else 'N/A'
end payment_type, fare_amount, extra extra_fees, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, airport_fee
from nyctaxitrips.bronze.yellow_taxi_trip
where cast(tpep_pickup_datetime as date) between '2024-01-01' and '2024-12-31'
