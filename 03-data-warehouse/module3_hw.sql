-- Testing Taxi 2024 date from source gcs
-- Check if the external table can actually see the files
SELECT count(*) as total_rows 
FROM `lydia-zoomcamp.zoomcamp.external_yellow_tripdata_2024`;
--q1 answer: 20332093

SELECT COUNT(*)
FROM `lydia-zoomcamp.zoomcamp.yellow_tripdata_2024_partitioned`
WHERE fare_amount = 0;
--q4: 8333