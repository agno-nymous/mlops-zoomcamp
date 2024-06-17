#!/usr/bin/env python
# coding: utf-8

import sys
import subprocess
import pickle
import pandas as pd

# Get year and month from command line arguments
year = int(sys.argv[1])
month = int(sys.argv[2])

# Get Scikit-Learn version
result = subprocess.run(['pip', 'freeze'], stdout=subprocess.PIPE)
installed_packages = result.stdout.decode('utf-8')
for line in installed_packages.split('\n'):
    if 'scikit-learn' in line:
        print(f'Scikit-Learn version: {line}')
        break

# Get Python version
result = subprocess.run(['python', '-V'], stdout=subprocess.PIPE)
print(f'Python version: {result.stdout.decode("utf-8")}')

# Load the model
with open('model.bin', 'rb') as f_in:
    dv, model = pickle.load(f_in)

categorical = ['PULocationID', 'DOLocationID']

def read_data(filename):
    df = pd.read_parquet(filename)
    
    df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
    
    return df

filename = f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month}.parquet'
df = read_data(filename)

dicts = df[categorical].to_dict(orient='records')
X_val = dv.transform(dicts)
y_pred = model.predict(X_val)

# Standard deviation of the predicted duration
print(f'Standard deviation of the predicted duration: {y_pred.std()}')

# Mean predicted duration
mean_duration = y_pred.mean()
print(f'Mean predicted duration: {mean_duration}')

# Preparing the output
df['ride_id'] = f'{year:04d}/{month:02d}_' + df.index.astype('str')
output_file = './04-deployment-output-file.parquet'
df_result = df.loc[:, ['ride_id', 'duration']]

df_result.to_parquet(
    output_file,
    engine='pyarrow',
    compression=None,
    index=False
)

# Print the size of the output file
result = subprocess.run(['ls', '-lh', output_file], stdout=subprocess.PIPE)
print(result.stdout.decode('utf-8'))