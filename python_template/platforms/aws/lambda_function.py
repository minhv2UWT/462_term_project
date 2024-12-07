import handler
import json
import boto3
import pandas as pd

#
# AWS Lambda Functions Default Function
#
# This hander is used as a bridge to call the platform neutral
# version in handler.py. This script is put into the scr directory
# when using publish.sh.
#
# @param request
#

s3_client = boto3.client('s3')
def lambda_handler(event, context):
	bucket = event['bucket']
	key = event['key']
	#downloading parquet file from s3
	local_path = '/local/path'
	s3_client.download_file(bucket, key, local_path)
	#process
	df = pd.read_parquet(local_file)
	#result = df.describe().to_dict() #example of processing, not necessary

	return {
		'statusCode': 200,
		'result': result
	}
	
	
