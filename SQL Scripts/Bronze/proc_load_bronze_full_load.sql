/*
=====================================================================================
Sript Purpose:
This stored procedure loads data into the bronze schema from external CSV files. 
It dynamically loads each file in the repository
It truncates the bronze tables before loading the data. This is a full load procedure
It Uses the 'bulk insert' to load data from csv files to the tables
Useful comment has been included
=====================================================================================
*/
create or alter procedure bronze.stp_load_bronze as 

begin 
    
    begin try 
        declare @start_time datetime,@end_time datetime;
        declare @prd_start_time datetime, @prd_end_time datetime;
        declare @path nvarchar(max) ;
        declare @file nvarchar(255);
        declare @cmd nvarchar(max);
        declare @row_count int;
        declare @table_name nvarchar(255);

        set @prd_start_time = getdate();
        print '======================================================================';
	    print 'Loading Bronze Layer';
	    print '======================================================================';

	    print '----------------------------------------------------------------------';
	    print 'Loading Yellow Trip Table';
	    print '----------------------------------------------------------------------';

        

            ---set the file path
            set @path = N'C:\Users\wuraola.akeeb\OneDrive - First City Monument Bank (FCMB)\Documents\PORTFOLIO\yellow_tripdata\csv_trip_data\'
            set @table_name = N'bronze.yellow_taxi_trip'

            --create a temporary table to store files for each month
            drop table if exists #files
            create table #files( filename nvarchar(255))
            insert into #files
            exec xp_cmdshell 'dir /b "C:\Users\wuraola.akeeb\OneDrive - First City Monument Bank (FCMB)\Documents\PORTFOLIO\yellow_tripdata\csv_trip_data\yellow_tripdata_2024-*.csv"'

            --truncate existing table
            print '>> Truncating Table: '+@table_name ;
	        exec('truncate table ' + @table_name)

            --create a cursor to iterate each file
            declare cur cursor for select filename from #files where filename is not null
            open cur
            fetch next from cur into @file

            --loop through all file
            while @@fetch_status = 0
            begin
                set @start_time = getdate();
                print '>> Inserting ' +@file +' into '+ @table_name;
                set @cmd = '                     
                    bulk insert '+ @table_name + '
                    from ''' + @path + @file + '''
                    with (
                        firstrow = 2,
                        fieldterminator = '','',
                        rowterminator = ''0x0a'',
                        tablock,
                        codepage = ''65001''
                    )
                '
                exec(@cmd);
                set @end_time = getdate();
                print '>> Load Duration for '+ @file +': ' + cast(datediff(second,@start_time,@end_time) as nvarchar) +' seconds'
                fetch next from cur into @file
            end

            close cur --stops the cursor from fetching more rows.
            deallocate cur --frees up memory used by the cursor.
            drop table #files

        set @prd_end_time = getdate();
        print '>> ======================================================================'
	    print '>> Load Duration of Bronze Layer: ' + cast(datediff(second,@prd_start_time,@prd_end_time) as nvarchar) +' seconds'
	    print '>> ======================================================================'
        set @row_count = (select count(*) from bronze.yellow_taxi_trip);

        insert into log_process_status(process_type,process_name,table_name,status,row_count,process_start_time,process_end_time,load_date) 
        values('stored procedure','Yellow Taxi Load',@table_name,'SUCCESS',@row_count,@prd_start_time,@prd_end_time,getdate())
    
    end try 
    begin catch 
        print '======================================================================';
        print 'Error Occured during Loading bronze layer';
        print '======================================================================';
        insert into log_process_status 
        values('stored procedure','Yellow Taxi Load',@table_name,'FAILED',@row_count,@prd_start_time,@prd_end_time,error_number(),error_message(),error_line(),error_procedure(),getdate())
    end catch
end
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

/* Loading the data manually 
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
*/
