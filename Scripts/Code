-- Create a new database to serve as the Data Warehouse
create database DataWareHouse;

-- Switch context to the newly created DataWareHouse database
use DataWareHouse;
go

-- Create the 'bronze' schema 
-- This layer stores raw, unprocessed data directly from source systems
create schema bronze;
go

-- Create the 'silver' schema 
-- This layer stores cleaned and transformed data after processing
create schema silver;
go

-- Create the 'gold' schema 
-- This layer stores aggregated, business-ready data for reporting and analytics
create schema gold;
go
