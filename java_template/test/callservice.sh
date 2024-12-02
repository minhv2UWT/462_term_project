#!/bin/bash
# JSON object to pass to Lambda Function
json={"\"row\"":50,"\"col\"":10,"\"bucketname\"":\"test.bucket.462562.f24.project\"","\"filename\"":\"taxi_zone_lookup.csv\""}
echo "Invoking Lambda function using AWS CLI (Boto3)"
time output=`aws lambda invoke --invocation-type RequestResponse --cli-binary-format raw-in-base64-out --function-name projectProcessCSV --region us-east-1 --payload $json /dev/stdout | head -n 1 | head -c -2 ; echo`
echo ""
echo "JSON RESULT:"
echo $output | jq
echo ""
