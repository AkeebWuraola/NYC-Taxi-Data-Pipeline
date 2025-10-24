/*
=====================================================================================
Sript Purpose:
This stored procedure loads data into the bronze schema from external CSV files. 
It automates the loading dynamically loading each file in the repository
--It truncates the bronze tables before loading the data
It Uses the 'bulk insert' to load data from csv files to the tables
Useful comment has been included
=====================================================================================
*/
declare @path nvarchar(max) 
declare @file nvarchar(255)
declare @cmd nvarchar(max)

set @path = N'C:\Users\wuraola.akeeb\OneDrive - First City Monument Bank (FCMB)\Documents\PORTFOLIO\yellow_tripdata\csv_trip_data\'

drop table if exists #files
create table #files( filename nvarchar(255))
insert into #files
exec xp_cmdshell 'dir /b "C:\Users\wuraola.akeeb\OneDrive - First City Monument Bank (FCMB)\Documents\PORTFOLIO\yellow_tripdata\csv_trip_data\yellow_tripdata_2024-*.csv"'

--create a cursor
declare cur cursor for select filename from #files where filename is not null
open cur
fetch next from cur into @file

while @@fetch_status = 0
begin
    set @cmd = '
    bulk insert bronze.yellow_taxi_trip
    from ''' + @path + @file + '''
    with (
        firstrow = 2,
        fieldterminator = '','',
        rowterminator = ''0x0a'',
        tablock,
        codepage = ''65001''
    )'
    exec(@cmd)
    fetch next from cur into @file
end

close cur --stops the cursor from fetching more rows.
deallocate cur --frees up memory used by the cursor.
drop table #files

/* Notes
The command in xp_cmdshell 'dir /b' lists file names only (bare format, without size/date info). 
A cursor is used to iterate through each filename in the temp table.

Each filename is fetched into the variable @file.


Error with xp_cmdshell:
SQL Server blocked access to procedure 'sys.xp_cmdshell' of component 'xp_cmdshell' because this component is turned off as part of the security configuration for this server. A system administrator can enable the use of 'xp_cmdshell' by using sp_configure. For more information about enabling 'xp_cmdshell', search for 'xp_cmdshell' in SQL Server Books Online.
Description:
means your SQL Server security settings have disabled xp_cmdshell, which is a system feature used to run OS-level commands (like reading files from disk).
Only if you’re the DBA or have admin rights — you can enable it safely like this:
Solution:
Run first:
-- enable advanced options
sp_configure 'show advanced options', 1;
reconfigure;
Run next:
-- enable xp_cmdshell
sp_configure 'xp_cmdshell', 1;
reconfigure;

--Revert settings disabling it again for security:
sp_configure 'xp_cmdshell', 0;
reconfigure;

*/
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
