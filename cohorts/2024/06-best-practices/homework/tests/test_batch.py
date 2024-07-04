import pytest
import pandas as pd
from batch import prepare_data
from datetime import datetime

def dt(hour, minute, second=0):
    return datetime(2023, 1, 1, hour, minute, second)

def test_prepare_data():
    data = [
        (None, None, dt(1, 1), dt(1, 10)), # duration = 9 mins
        (1, 1, dt(1, 2), dt(1, 10)), # duration = 8 mins
        (1, None, dt(1, 2, 0), dt(1, 2, 59)), # duration = 59 secs
        (3, 4, dt(1, 2, 0), dt(2, 2, 1)), # duration = 1 hour 1 sec
    ]
    
    columns = ['PULocationID', 'DOLocationID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime']
    
    df = pd.DataFrame(data, columns=columns)
    
    categorical = ['PULocationID', 'DOLocationID']
    actual_df = prepare_data(df, categorical)

    #keep only those rows where duration is between 1 and 60 mins
    expected_data = [
        ("-1", "-1", dt(1, 1), dt(1, 10), 9.0),
        ("1", "1", dt(1, 2), dt(1, 10), 8.0),
    ]
    
    expected_columns = ['PULocationID', 'DOLocationID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'duration']
    expected_df = pd.DataFrame(expected_data, columns=expected_columns)
    
    # Assert that there are 2 rows in the expected dataframe
    assert len(actual_df) == len(expected_df)
    # Assert that the actual dataframe is equal to the expected dataframe
    pd.testing.assert_frame_equal(actual_df, expected_df)


    import os
    options = {
        'client_kwargs': {
            'endpoint_url': os.getenv("S3_ENDPOINT_URL")
        }
    }
    df.to_parquet(
        "file.parquet",
        engine='pyarrow',
        compression=None,
        index=False,
        # storage_options=options
    )

if __name__ == "__main__":
    pytest.main()