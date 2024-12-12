import boto3
import csv

#
# AWS Lambda Functions Default Function
#
# This hander is used as a bridge to call the platform neutral
# version in handler.py. This script is put into the scr directory
# when using publish.sh.
#
# @param request
#


# Initialize S3 client
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    # Extract bucket and key from the event
    bucket = event['bucket']
    key = event['key']
    
    try:
        # Fetch the file content from S3
        response = s3_client.get_object(Bucket=bucket, Key=key)
        csv_content = response['Body'].read().decode('utf-8')  # Read and decode the file content
        
        # Process the CSV file directly in memory
        result = []
        csv_reader = csv.reader(csv_content.splitlines())
        headers = next(csv_reader)  # Extract headers
        for row in csv_reader:
            result.append(row)  # Append each row to the result list
        
        # Return the processed result
        return {
            'statusCode': 200,
            'headers': headers,
            'rows': result
        }
    
    except Exception as e:
        # Handle any exceptions
        return {
            'statusCode': 500,
            'error': str(e)
        }
