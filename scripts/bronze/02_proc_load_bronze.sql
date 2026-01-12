/*
============================================= A stored procedure to load data into Bronze Layer tables from CSV files uwsing a BULK INSERT and parameters. ================================================ */

CREATE OR ALTER PROCEDURE bronze.load_bronze 
	@data_path NVARCHAR(MAX) = 'C:\D-Data\1-Project\Data_Side_Project\Sql-data-warehouse-project\data\'
AS
BEGIN
	DECLARE @start_time DATETIME , @end_time DATETIME  , @batch_start_time DATETIME  , @batch_end_time DATETIME ;
	DECLARE @file_path NVARCHAR(MAX) ;
	DECLARE @sql NVARCHAR(MAX) ;

	BEGIN TRY
		SET @batch_start_time = GETDATE() ;

		PRINT'==================================='
		PRINT 'Loding Bronze Layer'
		PRINT'==================================='

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';

		SET @start_time = GETDATE() ;

		PRINT '>>Truncating Table : bronze.crm_cust_info' ;
		TRUNCATE TABLE bronze.crm_cust_info ;
		PRINT '>> Inserting Data into Table : bronze.crm_cust_info' ;
		SET @file_path = @data_path + 'source_crm\cust_info.csv' ;
		SET @sql = 'BULK INSERT bronze.crm_cust_info FROM ''' + @file_path + ''' WITH (FIRSTROW = 2, FIELDTERMINATOR = '','', TABLOCK)' ;
		EXECUTE sp_executesql @sql ;

		SET @end_time = GETDATE() ;
		PRINT '>> Time Taken to Load Table bronze.crm_cust_info : ' + CAST(DATEDIFF(SECOND , @start_time , @end_time) AS NVARCHAR(10)) + ' Seconds' ;

		SET @start_time = GETDATE() ;
		PRINT '>>Truncating Table : bronze.crm_prd_info' ;
		TRUNCATE TABLE bronze.crm_prd_info ;	
		PRINT '>> Inserting Data into Table : bronze.crm_prd_info' ;
		SET @file_path = @data_path + 'source_crm\prd_info.csv' ;
		SET @sql = 'BULK INSERT bronze.crm_prd_info FROM ''' + @file_path + ''' WITH (FIRSTROW = 2, FIELDTERMINATOR = '','', TABLOCK)' ;
		EXECUTE sp_executesql @sql ;
		SET @end_time = GETDATE() ;
		PRINT '>> Time Taken to Load Table bronze.crm_prd_info : ' + CAST(DATEDIFF(SECOND , @start_time , @end_time) AS NVARCHAR(10)) + ' Seconds' ;

		SET @start_time = GETDATE() ;
		PRINT '>>Truncating Table : bronze.crm_sales_details' ;
		TRUNCATE TABLE bronze.crm_sales_details ;
		PRINT '>> Inserting Data into Table : bronze.crm_sales_details' ;
		SET @file_path = @data_path + 'source_crm\sales_details.csv' ;
		SET @sql = 'BULK INSERT bronze.crm_sales_details FROM ''' + @file_path + ''' WITH (FIRSTROW = 2, FIELDTERMINATOR = '','', TABLOCK)' ;
		EXECUTE sp_executesql @sql ;
		SET @end_time = GETDATE() ;
		PRINT '>> Time Taken to Load Table bronze.crm_sales_details : ' + CAST(DATEDIFF(SECOND , @start_time , @end_time) AS NVARCHAR(10)) + ' Seconds' ;

		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';		
		PRINT '------------------------------------------------';
		
		SET	@start_time = GETDATE() ;
		PRINT '>>Truncating Table : bronze.erp_cust_az12' ;
		TRUNCATE TABLE bronze.erp_cust_az12 ;
		PRINT '>> Inserting Data into Table : bronze.erp_cust_az12' ;
		SET @file_path = @data_path + 'source_erp\CUST_AZ12.csv' ;
		SET @sql = 'BULK INSERT bronze.erp_cust_az12 FROM ''' + @file_path + ''' WITH (FIRSTROW = 2, FIELDTERMINATOR = '','', TABLOCK)' ;
		EXECUTE sp_executesql @sql ;
		SET @end_time = GETDATE() ;
		PRINT '>> Time Taken to Load Table bronze.erp_cust_az12 : ' + CAST(DATEDIFF(SECOND , @start_time , @end_time) AS NVARCHAR(10)) + ' Seconds' ;

		SET	@start_time = GETDATE() ;
		PRINT '>>Truncating Table : bronze.erp_prd_az12' ;
		TRUNCATE TABLE bronze.erp_prd_az12 ;
		PRINT '>> Inserting Data into Table : bronze.erp_prd_az12' ;
		SET @file_path = @data_path + 'source_erp\LOC_A101.csv' ;
		SET @sql = 'BULK INSERT bronze.erp_prd_az12 FROM ''' + @file_path + ''' WITH (FIRSTROW = 2, FIELDTERMINATOR = '','', TABLOCK)' ;
		EXECUTE sp_executesql @sql ;
		SET @end_time = GETDATE() ;
		PRINT '>> Time Taken to Load Table bronze.erp_prd_az12 : ' + CAST(DATEDIFF(SECOND , @start_time , @end_time) AS NVARCHAR(10)) + ' Seconds' ;

		SET	@start_time = GETDATE() ;
		PRINT '>>Truncating Table : bronze.erp_sales_az12' ;
		TRUNCATE TABLE bronze.erp_sales_az12 ;
		PRINT '>> Inserting Data into Table : bronze.erp_sales_az12' ;
		SET @file_path = @data_path + 'source_erp\PX_CAT_G1V2.csv' ;
		SET @sql = 'BULK INSERT bronze.erp_sales_az12 FROM ''' + @file_path + ''' WITH (FIRSTROW = 2, FIELDTERMINATOR = '','', TABLOCK)' ;
		EXECUTE sp_executesql @sql ;
		SET @end_time = GETDATE() ;
		PRINT '>> Time Taken to Load Table bronze.erp_sales_az12 : ' + CAST(DATEDIFF(SECOND , @start_time , @end_time) AS NVARCHAR(10)) + ' Seconds' ;
		SET @batch_end_time = GETDATE() ;
		PRINT '==================================='
		PRINT '✓ Bronze Layer Loaded Successfully in ' + CAST(DATEDIFF(SECOND , @batch_start_time , @batch_end_time) AS NVARCHAR(10)) + ' Seconds'
		PRINT '==================================='
	END TRY

	BEGIN CATCH	
		PRINT'==========================================='
		PRINT '!!! Error Occurred During Bronze Layer Load !!!' ;
		PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR(10)) ;
		PRINT 'Error Message: ' + ERROR_MESSAGE() ;
		PRINT'==========================================='
	END CATCH

END;






EXEC bronze.load_bronze @data_path = 'C:\D-Data\1-Project\Data_Side_Project\Sql-data-warehouse-project\data\';