import os
import requests
from google.cloud import storage

# Configuration
BUCKET_NAME = "lydia-zoomcamp-kestra-bucket"  # Update this
PROJECT_ID = "lydia-zoomcamp"
YEAR = 2019
BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv"

def upload_to_gcs(bucket_name, object_name, local_file):
    """Uploads a file to the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)
    print(f"File {local_file} uploaded to {object_name}.")

def download_and_upload():
    for month in range(1, 13):
        # Format month to 01, 02, etc.
        month_str = f"{month:02d}"
        file_name = f"fhv_tripdata_{YEAR}-{month_str}.csv.gz"
        request_url = f"{BASE_URL}/{file_name}"
        
        # Download the file
        print(f"Downloading {request_url}...")
        r = requests.get(request_url)
        
        if r.status_code == 200:
            with open(file_name, 'wb') as f:
                f.write(r.content)
            
            # Upload to GCS (into an 'fhv' folder)
            gcs_path = f"fhv/{file_name}"
            upload_to_gcs(BUCKET_NAME, gcs_path, file_name)
            
            # Clean up local file
            os.remove(file_name)
        else:
            print(f"Could not download {file_name}")

if __name__ == "__main__":
    download_and_upload()