import os
import pandas as pd
from datetime import datetime
from batch import get_input_path, get_output_path, prepare_data
from dotenv import load_dotenv
import subprocess
import pytest

load_dotenv(".env")

#Def constances
S3_ENDPOINT_URL = os.environ.get("S3_ENDPOINT_URL", "http://localhost:4566")

def dt(hour, minute, second=0):
    return datetime(2023, 1, 1, hour, minute, second)

def get_storage_options():
    return {
        'client_kwargs': {
            'endpoint_url': S3_ENDPOINT_URL
        }
    }

def create_bucket(bucket):
    #check if bucket exists
    command = f"aws --endpoint-url={S3_ENDPOINT_URL} s3api head-bucket --bucket {bucket}"

    if os.system(command) != 0: #if bucket does not exist
        os.system(
            f"aws --endpoint-url={S3_ENDPOINT_URL} s3 mb s3://{bucket}"
        )
    else:
        print(f"Bucket {bucket} already exists")

def save_test_data():
    # Create Localstack bucket
    create_bucket(bucket)

    # Create the dataframe
    data = [
        (None, None, dt(1, 1), dt(1, 10)),
        (1, 1, dt(1, 2), dt(1, 10)),
        (1, None, dt(1, 2, 0), dt(1, 2, 59)),
        (3, 4, dt(1, 2, 0), dt(2, 2, 1)),
    ]
    columns = ['PULocationID', 'DOLocationID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime']
    df_input = pd.DataFrame(data, columns=columns)

    # Save the dataframe to S3
    options = get_storage_options()

    df_input.to_parquet(
        input_file,
        engine='pyarrow',
        compression=None,
        index=False,
        storage_options=options
    )
    print(f"Test data saved to {input_file}")


def run_batch_script(year, month):
    command = f"python3 batch.py {year} {month}"
    subprocess.run(command, shell=True, check=True)

def test_integration():
    year = 2023
    month = 1
    output_file = get_output_path(year, month)

    # Read data from S3
    options = get_storage_options()
    df_result = pd.read_parquet(output_file, storage_options=options)

    # predicted mean duration round to 2 decimal places: 18.14
    assert round(df_result['predicted_duration'].mean(), 2) == 18.14
    # predicted sum of duration: 36.28
    assert round(df_result['predicted_duration'].sum(), 2) == 36.28

if __name__ == "__main__":
    # Define the S3 bucket and file paths
    bucket = "nyc-duration"
    input_file = get_input_path(2023, 1)
    output_file = get_output_path(2023, 1)
    # save_test_data()
    run_batch_script(2023, 1)
    pytest.main()