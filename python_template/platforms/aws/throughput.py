import boto3
import time
import requests
import csv
import matplotlib
import matplotlib.pyplot as plt
import io
import json
import concurrent.futures

matplotlib.use('Agg')

lambda_client = boto3.client('lambda')

def download_and_measure_rows(presigned_url):
    response = requests.get(presigned_url)
    response.raise_for_status()  # Raise an error if the request fails
    csv_content = response.content.decode('utf-8')
    rows = 0
    # Use csv.reader to count rows
    for _ in csv.reader(csv_content.splitlines(), delimiter=','):
        rows += 1
    return rows

def invoke_lambda_with_url(url):
    start_time = time.time()
    response = lambda_client.invoke(
        FunctionName="your_lambda_function_name", 
        InvocationType='RequestResponse',
        Payload=json.dumps({"url": url})  # Send the presigned URL as an event
    )
    end_time = time.time()

    # Process Lambda response to extract row count
    payload = json.loads(response['Payload'].read())
    rows_processed = payload.get('rows_processed', 0)  # Assumes Lambda returns this
    duration = end_time - start_time
    throughput = rows_processed / duration if duration > 0 else 0
    return throughput

# Function to perform throughput test
def throughput_test(presigned_urls, concurrency_levels):
    results = []
    for concurrency in concurrency_levels:
        throughputs = []
        print(f"Testing with concurrency level: {concurrency}")
        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = [
                executor.submit(invoke_lambda_with_url, url)
                for url in presigned_urls
            ]
            for future in concurrent.futures.as_completed(futures):
                throughputs.append(future.result())
        avg_throughput = sum(throughputs) / len(throughputs)
        results.append((concurrency, avg_throughput))
        print(f"Average throughput for concurrency {concurrency}: {avg_throughput:.2f} rows/second")
    return results

# Function to plot and upload results
def plot_throughput_results(results, bucket_name, output_key):
    concurrencies = [result[0] for result in results]
    avg_throughputs = [result[1] for result in results]
    plt.figure(figsize=(10, 6))
    plt.plot(concurrencies, avg_throughputs, marker='o')
    plt.xlabel('Concurrency Level')
    plt.ylabel('Average Throughput (rows/second)')
    plt.title('Lambda Function Throughput Test')
    plt.grid(True)

    # Save plot to in-memory file
    img_data = io.BytesIO()
    plt.savefig(img_data, format='png')
    img_data.seek(0)

    # Upload the plot to S3
    s3_client = boto3.client('s3')
    s3_client.put_object(Bucket=bucket_name, Key=output_key, Body=img_data, ContentType='image/png')
    print(f"Plot uploaded to S3: s3://{bucket_name}/{output_key}")

# Lambda handler
def lambda_handler(event, context):
    # Extract parameters from the event
    presigned_urls = event['presigned_urls']  # List of presigned URLs for CSV files
    concurrency_levels = event['concurrency_levels']  # Concurrency levels to test
    s3_bucket = event['s3_bucket']  # S3 bucket for storing the plot
    output_key = event['output_key']  # Key for the plot in S3

    if not presigned_urls:
        raise ValueError("No presigned URLs provided in the event payload.")

    # Run throughput test
    results = throughput_test(presigned_urls, concurrency_levels)

    # Plot and upload results to S3
    plot_throughput_results(results, s3_bucket, output_key)

    return {
        "statusCode": 200,
        "message": f"Throughput test completed. Results uploaded to s3://{s3_bucket}/{output_key}"
    }
