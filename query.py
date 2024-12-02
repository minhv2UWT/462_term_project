import boto3
import pandas as pd
from io import StringIO

# Initialize boto3 client for S3
s3_client = boto3.client('s3')

def read_data_from_s3(bucket_name, s3_key):
    """Function to read CSV data from S3."""
    try:
        # Get the object from S3
        obj = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
        
        # Read the CSV data into a DataFrame
        data = obj['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(data))
        return df
    except Exception as e:
        print(f"Error reading data from S3: {e}")
        return None

def query_data(df, column, value):
    """Query the dataframe based on a column and value."""
    try:
        queried_data = df[df[column] == value]
        return queried_data
    except KeyError:
        print(f"Column {column} does not exist in the dataframe.")
        return None

def main():
    # Define your S3 bucket and key for input data
    input_bucket_name = ''  # Replace with your actual bucket name
    input_s3_key = 's3_key' # Replace with your actual key
    
    # Step 1: Read data from S3
    df = read_data_from_s3(input_bucket_name, input_s3_key)
    if df is None:
        print("Failed to load data from S3.")
        return

    # Step 2: Query the data (example query)
    # Let's assume you want to filter by Borough (e.g., 'Manhattan')
    borough_to_query = 'Manhattan'  # Change this to your desired borough
    queried_data = query_data(df, 'Borough', borough_to_query)
    
    if queried_data is not None and not queried_data.empty:
        print("Queried Data:")
        print(queried_data)
    else:
        print(f"No data found for {borough_to_query}.")

if __name__ == "__main__":
    main()
