/*
  FILE: 02_dq_audit_yellow_tripdata_2024.sql
  PURPOSE: Validate integrity of 2024 NYC TAXI DATA after GCS load.
  Target Table: lydia-zoomcamp.zoomcamp.yellow_tripdata_2024_partitioned
  AUTHOR: Lydia Liu (Data Engineer)
  DATE: 2024-02-09
  EXPECTED ROW COUNT: 20, 332, 093
*/

-- 1. THE VOLUME CHECK
-- Ensures we didn't lose data during the GCS-to-BigQuery transfer.
SELECT 
    'Volume Check' AS test_name,
    COUNT(*) AS result,
    CASE WHEN COUNT(*) = 20332093 THEN 'PASS' ELSE 'FAIL' END AS status
FROM `lydia-zoomcamp.zoomcamp.yellow_tripdata_2024_partitioned`;

---

-- 2. THE PARTITION INTEGRITY CHECK
-- Checks for "ghost" data. Taxi meters are notorious for having wrong dates (e.g., year 2008 or 2099).
-- This ensures all data actually belongs in 2024.
SELECT
    'Out-of-Range Dates' AS test_name,
    COUNT(*) AS bad_rows,
    'Rows found outside Jan-June 2024' AS description
FROM `lydia-zoomcamp.zoomcamp.yellow_tripdata_2024_partitioned`
WHERE tpep_pickup_datetime < '2024-01-01' OR tpep_pickup_datetime > '2024-06-30';

---

-- 3. THE "IMPOSSIBLE TRIP" CHECK (Logic Test)
-- In the real world, you can't drop off a passenger before you pick them up.
-- This catches technical glitches in the data collection.
SELECT
    'Negative Duration Trips' AS test_name,
    COUNT(*) AS bad_rows,
    'Dropoff occurs before Pickup' AS description
FROM `lydia-zoomcamp.zoomcamp.yellow_tripdata_2024_partitioned`
WHERE tpep_dropoff_datetime < tpep_pickup_datetime;

---

-- 4. THE ZERO-PASSENGER CHECK (Completeness Test)
-- Identify if there's a significant amount of data with 0 passengers. 
-- While "0" might be valid for some reasons, it's often a sign of sensor failure.
SELECT
    'Zero Passenger Count' AS test_name,
    COUNT(*) AS row_count,
    ROUND(COUNT(*) * 100 / 20332093, 2) AS percent_of_total
FROM `lydia-zoomcamp.zoomcamp.yellow_tripdata_2024_partitioned`
WHERE passenger_count = 0;

---

-- 5. THE FINANCIAL SANITY CHECK
-- Checks for negative fares or impossible tip amounts. 
-- Total_amount should generally be positive.
SELECT
    'Financial Anomaly' AS test_name,
    COUNT(*) AS bad_rows,
    'Negative or zero total amount' AS description
FROM `lydia-zoomcamp.zoomcamp.yellow_tripdata_2024_partitioned`
WHERE total_amount <= 0;

---

-- 6. NULL VALUE AUDIT (The "Swiss Cheese" Test)
-- Checks if critical columns for partitioning or billing are missing data.
SELECT
    'Null Column Check' AS test_name,
    COUNTIF(VendorID IS NULL) AS null_vendors,
    COUNTIF(tpep_pickup_datetime IS NULL) AS null_pickups,
    COUNTIF(PULocationID IS NULL) AS null_locations
FROM `lydia-zoomcamp.zoomcamp.yellow_tripdata_2024_partitioned`;