-- Bucket name: lydia-zoomcamp-kestra-bucket
-- Project ID:  lydia-zoomcamp


-- 1. Creating EXTERNAL table referring to your GCS path
-- Note: format is now PARQUET and path uses your bucket name
CREATE OR REPLACE EXTERNAL TABLE `lydia-zoomcamp.zoomcamp.external_yellow_tripdata_2024`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://lydia-zoomcamp-kestra-bucket/yellow/yellow_tripdata_2024-*.parquet']
);

-- 2. Create a NON-PARTITIONED table (Native BQ Table)
CREATE OR REPLACE TABLE `lydia-zoomcamp.zoomcamp.yellow_tripdata_2024_non_partitioned` AS
SELECT * FROM `lydia-zoomcamp.zoomcamp.external_yellow_tripdata_2024`;


-- 3. Create a PARTITIONED table
-- Partitioning by day helps with Question 2's cost analysis
CREATE OR REPLACE TABLE `lydia-zoomcamp.zoomcamp.yellow_tripdata_2024_partitioned`
PARTITION BY
  DATE(tpep_pickup_datetime) AS
SELECT * FROM `lydia-zoomcamp.zoomcamp.external_yellow_tripdata_2024`;


-- 4. Create a PARTITIONED and CLUSTERED table
-- This is the most optimized version for 2024 data
CREATE OR REPLACE TABLE `lydia-zoomcamp.zoomcamp.yellow_tripdata_2024_partitioned_clustered`
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `lydia-zoomcamp.zoomcamp.external_yellow_tripdata_2024`;