#!/bin/bash

set -e

TAXI_TYPE="yellow"
YEAR=2025 # Note: 2025 data might only be available for the first few months.

# Official NYC TLC CloudFront URL
URL_PREFIX="https://d37ci6vzurychx.cloudfront.net/trip-data"

for MONTH in {1..12}; do
  FMONTH=$(printf "%02d" ${MONTH})

  # File naming follows: yellow_tripdata_2024-01.parquet
  URL="${URL_PREFIX}/${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.parquet"

  LOCAL_PREFIX="data/raw/${TAXI_TYPE}/${YEAR}/${FMONTH}"
  LOCAL_FILE="${TAXI_TYPE}_tripdata_${YEAR}_${FMONTH}.parquet"
  LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_FILE}"

  if [ -f "$LOCAL_PATH" ]; then
    echo "File ${LOCAL_PATH} already exists, skipping..."
  else
    echo "Downloading ${URL} to ${LOCAL_PATH}..."
    mkdir -p ${LOCAL_PREFIX}
    
    # Using curl -L (L follows redirects which is common with cloudfront)
    curl -L ${URL} -o ${LOCAL_PATH} || echo "Failed to download ${URL}"
  fi
done