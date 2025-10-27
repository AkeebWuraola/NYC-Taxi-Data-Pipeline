select distinct store_and_fwd_flag from nyctaxitrips.bronze.yellow_taxi_trip

select distinct payment_type from nyctaxitrips.bronze.yellow_taxi_trip

select distinct ratecodeid from nyctaxitrips.bronze.yellow_taxi_trip

select * from nyctaxitrips.bronze.yellow_taxi_trip where tip_amount <0
tip amount in negative**


--check for invalid pickup date or outliers -- drop off might be valid as trips can spill over to new year
select min(tpep_pickup_datetime),max(tpep_pickup_datetime) from nyctaxitrips.bronze.yellow_taxi_trip 

select * from nyctaxitrips.bronze.yellow_taxi_trip  
where cast(tpep_pickup_datetime as date) <'2024-01-01' or cast(tpep_pickup_datetime as date) >'2024-12-31'


calculate trip duration

	/* Data Transformations Performed
	Data Normalization & Standardization: Mapping coded values to meaningful, user-friendly descriptions
	Handling Missing Dtaa: fill in the blanks by adding default value
	Derived Columns: create new columns based on calculations or transformation of existing columns e.g calculating trip duration
	Data Enrichment: Add new, relevant data to enhance the dataset for analysis
	Removed invalid values
	*/
