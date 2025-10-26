/*
=====================================================================================
Sript Purpose:
This stored procedure loads data incrementally into the bronze schema from external CSV files. 
It dynamically loads each file in the repository
It checks if the data has already been loaded using the file log before loading the data
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
	    print 'Incremental Loading Bronze Layer';
	    print '======================================================================';

	    print '----------------------------------------------------------------------';
	    print 'Updating Yellow Trip Table';
	    print '----------------------------------------------------------------------';

        

            ---set the file path
            set @path = N'C:\Users\wuraola.akeeb\OneDrive - First City Monument Bank (FCMB)\Documents\PORTFOLIO\yellow_tripdata\csv_trip_data\'
            set @table_name = N'bronze.yellow_taxi_trip'

            --create a temporary table to store files for each month
            drop table if exists #files
            create table #files( filename nvarchar(255))
            insert into #files
            exec xp_cmdshell 'dir /b "C:\Users\wuraola.akeeb\OneDrive - First City Monument Bank (FCMB)\Documents\PORTFOLIO\yellow_tripdata\csv_trip_data\yellow_tripdata_2024-*.csv"'
            
            -- Step 2: Filter only NEW (unprocessed) files
            delete from #files
            where filename is null
            or filename in (select file_name from bronze.etl_file_log where table_name = @table_name and load_status='success');

            if not exists (select 1 from #files)
            begin
                print '>> No new files to process. Exiting.';
                return;
            end
           
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
                begin try
                    exec(@cmd);
                    set @end_time = getdate();
                    print '>> Load Duration for '+ @file +': ' + cast(datediff(second,@start_time,@end_time) as nvarchar) +' seconds'
                    set @row_count = @@rowcount; ---@@rowcount is a function that counts rows affected by the last sql run
                    ---Log successful file load
                    insert into bronze.etl_file_log
                    values(@file,@table_name,@row_count,'success',getdate())
                end try
                begin catch
                    ---Log failed file load
                    insert into bronze.etl_file_log
                    values(@file,@table_name,0,'failed',getdate())
                    print '!! Error loading file: ' + @file + ' - ' + error_message();
                end catch

                fetch next from cur into @file
            end

            close cur --stops the cursor from fetching more rows.
            deallocate cur --frees up memory used by the cursor.
            drop table #files

        set @prd_end_time = getdate();
        print '>> ======================================================================'
	    print '>> Incremental Load Duration of Bronze Layer: ' + cast(datediff(second,@prd_start_time,@prd_end_time) as nvarchar) +' seconds'
	    print '>> ======================================================================'
        set @row_count = (select count(*) from bronze.yellow_taxi_trip);

        insert into log_process_status(process_type,process_name,table_name,status,row_count,process_start_time,process_end_time,load_date) 
        values('stored procedure','Yellow Taxi Load',@table_name,'SUCCESS',@row_count,@prd_start_time,@prd_end_time,getdate())
    
    end try 
    begin catch 
        print '======================================================================';
        print 'Error Occured during Incremental Load of the bronze layer';
        print '======================================================================';
        insert into log_process_status 
        values('stored procedure','Yellow Taxi Load',@table_name,'FAILED',@row_count,@prd_start_time,@prd_end_time,error_number(),error_message(),error_line(),error_procedure(),getdate())
    end catch
end
