bulk insert bronze.yellow_taxi_trip
from 'C:\Users\wuraola.akeeb\OneDrive - First City Monument Bank (FCMB)\Documents\PORTFOLIO\yellow_tripdata\csv_trip_data\yellow_tripdata_2024-01.csv'
with (
	firstrow = 2, --specifies the beginning of your data
	fieldterminator = ',', --specifies the delimeter
	rowterminator = '0x0a', -- The row terminator is \n
	tablock --locks the table while insert is ongoing
);

bulk insert bronze.green_taxi_trip
from 'C:\Users\wuraola.akeeb\OneDrive - First City Monument Bank (FCMB)\Documents\PORTFOLIO\green_tripdata\csv_trip_data\green_tripdata_2024-01.csv'
with (
	firstrow = 2, --specifies the beginning of your data
	fieldterminator = ',', --specifies the delimeter
	rowterminator = '0x0a',	--The row terminator is \n
	tablock --locks the table while insert is ongoing
);
