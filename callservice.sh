#!/bin/bash
# JSON object to pass to Lambda Function
json={"\"row\"":100,"\"col\"":25,"\"bucketname\"":\"test.bucket.462562.f24.project\"","\"filename\"":\"trips.csv\""}
echo "Invoking Lambda function Extract and Transform using AWS CLI (Boto3)"
time output=`aws lambda invoke --invocation-type RequestResponse --cli-binary-format raw-in-base64-out --function-name extractTransform --region us-east-1 --payload $json /dev/stdout | head -n 1 | head -c -2 ; echo`
echo ""
echo "JSON RESULT:"
echo $output | jq
echo ""

# JSON object to pass to Lambda Function
json={"\"row\"":100,"\"col\"":25,"\"bucketname\"":\"test.bucket.462562.f24.project\"","\"filename\"":\"transformed_trips.csv\""}
echo "Invoking Lambda function Load using AWS CLI (Boto3)"
time output=`aws lambda invoke --invocation-type RequestResponse --cli-binary-format raw-in-base64-out --function-name Load --region us-east-1 --payload $json /dev/stdout | head -n 1 | head -c -2 ; echo`
echo ""
echo "JSON RESULT:"
echo $output | jq
echo ""

# JSON object to pass to Lambda Function
json='{
  "row": 100,
  "col": 25,
  "bucketname": "test.bucket.462562.f24.project",
  "filename": "transformed_trips.csv",
  "groupByColumns": ["hvfhs_license_num"],
  "aggregationFunction": "COUNT",
  "whereClause": "trip_miles > 0"
}'

echo "Invoking Lambda function Query using AWS CLI (Boto3)"
time output=$(aws lambda invoke \
  --invocation-type RequestResponse \
  --cli-binary-format raw-in-base64-out \
  --function-name Query \
  --region us-east-1 \
  --payload "$json" /dev/stdout | head -n 1 | head -c -2 ; echo)

echo ""
echo "JSON RESULT:"
echo $output | jq
echo ""




