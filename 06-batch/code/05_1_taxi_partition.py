
# Without enforcement of schema, partition the input data


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. Initialize Spark Session
spark = SparkSession.builder \
    .appName("NYC_Taxi_Preprocessing") \
    .master("local[*]") \
    .getOrCreate()

def process_taxi_data(taxi_type, year):
    """
    Reads Parquet files, standardizes columns, and saves as repartitioned Parquet.
    """
    for month in range(1, 13):
        print(f"--- Processing {taxi_type} taxi for {year}-{month:02d} ---")
        
        input_path = f"data/raw/{taxi_type}/{year}/{month:02d}/*.parquet"
        output_path = f"data/pq/{taxi_type}/{year}/{month:02d}/"

        try:
            # 2. Read the official Parquet file (Schema is auto-detected)
            df = spark.read.parquet(input_path)

            # 3. Standardize column names (Very common in real pipelines)
            # Yellow and Green have different names for pickup/dropoff. We unify them.
            if taxi_type == 'yellow':
                df = df.withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
                       .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')
            elif taxi_type == 'green':
                df = df.withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
                       .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')

            # 4. Filter for Data Quality (Industry Insight: Sanity Checks)
            # Remove trips with 0 distance or impossible dates
            df = df.filter((df.trip_distance > 0) & (df.passenger_count > 0))

            # 5. Write to local directory with 4 partitions
            # 'overwrite' mode ensures you can re-run the script safely.
            df.repartition(4) \
              .write \
              .mode("overwrite") \
              .parquet(output_path)
            
            print(f"Successfully saved to {output_path}")

        except Exception as e:
            print(f"Skipping {year}-{month:02d}: {e}")

# Run for both types
process_taxi_data('yellow', 2025)
# process_taxi_data('green', 2024) # Uncomment if you have green taxi data too