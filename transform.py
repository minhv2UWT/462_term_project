import boto3
import pandas as pd
from io import BytesIO

def read_data_from_s3(bucket_name, s3_key):
    s3_client = boto3.client('s3')
    obj = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
    return pd.read_csv(BytesIO(obj['Body'].read()))

def transform_data(data):
    data.columns = data.columns.str.lower().str.replace(' ', '_')
    if 'date' in data.columns and 'time' in data.columns:
        data['datetime'] = pd.to_datetime(data['date'] + ' ' + data['time'])
    data = data.drop_duplicates()
    if 'pickup_latitude' in data.columns and 'pickup_longitude' in data.columns:
        data = data[(data['pickup_latitude'].between(-90, 90)) & 
                    (data['pickup_longitude'].between(-180, 180))]
    numeric_cols = data.select_dtypes(include=['float64', 'int64']).columns
    data[numeric_cols] = data[numeric_cols].fillna(data[numeric_cols].median())
    return data

def save_transformed_data_to_s3(data, bucket_name, output_s3_key):
    csv_buffer = BytesIO()
    data.to_csv(csv_buffer, index=False)
    s3_client = boto3.client('s3')
    s3_client.put_object(Bucket=bucket_name, Key=output_s3_key, Body=csv_buffer.getvalue())
    print(f"Transformed data saved to s3://{bucket_name}/{output_s3_key}")

if __name__ == "__main__":
    input_bucket_name = "your-input-bucket-name"
    input_s3_key = "path/to/uber_data.csv"
    output_bucket_name = "your-output-bucket-name"
    output_s3_key = "path/to/transformed_uber_data.csv"
    data = read_data_from_s3(input_bucket_name, input_s3_key)
    transformed_data = transform_data(data)
    save_transformed_data_to_s3(transformed_data, output_bucket_name, output_s3_key)
