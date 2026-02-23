# Only process 2025 yellow data, no 2025 green taxi data from official.

#!/usr/bin/env python
# coding: utf-8

import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main(params):
    input_yellow = params.input_yellow
    output = params.output
    temp_gcs_bucket = params.temp_gcs_bucket

    # 1. Initialize Spark Session
    spark = SparkSession.builder \
        .appName('yellow-taxi-2025-to-bq') \
        .getOrCreate()

    # BigQuery staging area
    spark.conf.set('temporaryGcsBucket', temp_gcs_bucket)

    # 2. Loading 2025 Yellow Data from GCS
    print(f"Loading Yellow taxi data from: {input_yellow}")
    df_yellow = spark.read.parquet(input_yellow)

    # 3. Standardization (Matching the 2025 official Parquet column names)
    # We rename to 'pickup_datetime' to keep your SQL clean and reusable
    df_yellow = df_yellow.withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
                         .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')

    # 4. Prepare for SQL
    df_yellow.createOrReplaceTempView('yellow_trips')

    # Aggregation SQL: Monthly revenue and passenger metrics
    df_result = spark.sql("""
    SELECT 
        PULocationID AS revenue_zone,
        date_trunc('month', pickup_datetime) AS revenue_month, 
        SUM(fare_amount) AS revenue_monthly_fare,
        SUM(extra) AS revenue_monthly_extra,
        SUM(mta_tax) AS revenue_monthly_mta_tax,
        SUM(tip_amount) AS revenue_monthly_tip_amount,
        SUM(tolls_amount) AS revenue_monthly_tolls_amount,
        SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
        SUM(total_amount) AS revenue_monthly_total_amount,
        SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge,
        AVG(passenger_count) AS avg_monthly_passenger_count,
        AVG(trip_distance) AS avg_monthly_trip_distance
    FROM
        yellow_trips
    GROUP BY
        1, 2
    """)

    # 5. Write to BigQuery
    print(f"Writing results to BigQuery: {output}")
    df_result.write.format('bigquery') \
        .option('table', output) \
        .mode('overwrite') \
        .save()
    
    print("Job completed successfully!")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_yellow', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--temp_gcs_bucket', required=True)

    args = parser.parse_args()
    main(args)