
# web_to_gcs_v file created and modified by Lydia


import os
import requests
import pandas as pd
from google.cloud import storage

# 1. NEW SOURCE URL
init_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'
# Switch out your bucket name here
BUCKET = os.environ.get("GCP_GCS_BUCKET", "lydia-zoomcamp-kestra-bucket")

def upload_to_gcs(bucket, object_name, local_file):
    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

def web_to_gcs(year, service):
    # For 2024, data is currently available for months 01 to 06
    for i in range(6): 
        month = f"{i+1:02d}"

        # 2. CHANGE EXTENSION TO PARQUET
        file_name = f"{service}_tripdata_{year}-{month}.parquet"
        request_url = f"{init_url}{file_name}"
        
        print(f"Downloading {request_url}...")
        r = requests.get(request_url)
        
        if r.status_code == 200:
            with open(file_name, 'wb') as f:
                f.write(r.content)
            print(f"Local: {file_name}")

            # 3. UPLOAD DIRECTLY (No need to convert CSV to Parquet)
            upload_to_gcs(BUCKET, f"{service}/{file_name}", file_name)
            print(f"GCS: {service}/{file_name}")
            
            # Optional: Remove local file to save space
            os.remove(file_name)
        else:
            print(f"File not found: {request_url} (Status: {r.status_code})")

# Run for Yellow 2024
web_to_gcs('2024', 'yellow')