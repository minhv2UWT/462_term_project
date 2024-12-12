import boto3
import time
import requests
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
import io
import json
import concurrent.futures


matplotlib.use('Agg')

lambda_client = boto3.client('lambda')

# Pre-signed URLs for the CSV files
PRESIGNED_URLS = [
    "https://462-proj-dataset.s3.us-east-1.amazonaws.com/fhvhv_tripdata_2021-01.csv?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAQ3EGTNWWJQM6KGE7%2F20241212%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20241212T194532Z&X-Amz-Expires=604800&X-Amz-SignedHeaders=host&X-Amz-Signature=041ef60d7f058bd9585315cc054457ea750976e543bb81be1c0c68e79f7bbd57",
    "https://462-proj-dataset.s3.us-east-1.amazonaws.com/fhvhv_tripdata_2021-02.csv?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAQ3EGTNWWJQM6KGE7%2F20241212%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20241212T194532Z&X-Amz-Expires=604800&X-Amz-SignedHeaders=host&X-Amz-Signature=2c00c0387114fde5a9c2e02d8c8c0c6e00be56fa942b86d0521c70fa1c53243e",
    "https://462-proj-dataset.s3.us-east-1.amazonaws.com/fhvhv_tripdata_2021-03.csv?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAQ3EGTNWWJQM6KGE7%2F20241212%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20241212T194532Z&X-Amz-Expires=604800&X-Amz-SignedHeaders=host&X-Amz-Signature=78d40729cd3116c1dc8e81365852f858e352d19d4c134c675136437acc51b2c0",
]

# Track cold start
cold_start_invoked = False

def invoke_lambda_with_metrics(url, is_cold_start=False):
    global cold_start_invoked
    start_time = time.time()
    response = lambda_client.invoke(
        FunctionName="your_lambda_function_name",
        InvocationType='RequestResponse',
        Payload=json.dumps({"url": url})  # Send the presigned URL as an event
    )
    end_time = time.time()

    # Extract Lambda response
    payload = json.loads(response['Payload'].read())
    rows_processed = payload.get('rows_processed', 0)  # Assumes Lambda returns this
    duration = end_time - start_time
    throughput = rows_processed / duration if duration > 0 else 0

    if is_cold_start and not cold_start_invoked:
        cold_start_invoked = True
        return {"type": "cold", "duration": duration, "throughput": throughput}
    else:
        return {"type": "warm", "duration": duration, "throughput": throughput}


def throughput_test(presigned_urls, concurrency_levels):
    results = {
        "cold": [],
        "warm": [],
        "scale_test": [],
    }

    for concurrency in concurrency_levels:
        throughputs = []
        runtimes = []
        print(f"Testing with concurrency level: {concurrency}")

        # Trigger cold starts initially
        cold_results = [
            invoke_lambda_with_metrics(url, is_cold_start=True) for url in presigned_urls
        ]
        results["cold"].extend(cold_results)

        # Perform concurrent warm invocations
        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = [
                executor.submit(invoke_lambda_with_metrics, url, is_cold_start=False)
                for url in presigned_urls
            ]
            for future in concurrent.futures.as_completed(futures):
                res = future.result()
                runtimes.append(res["duration"])
                throughputs.append(res["throughput"])

        avg_runtime = sum(runtimes) / len(runtimes)
        avg_throughput = sum(throughputs) / len(throughputs)

        results["scale_test"].append({
            "concurrency": concurrency,
            "avg_runtime": avg_runtime,
            "avg_throughput": avg_throughput
        })
        results["warm"].append({"avg_runtime": avg_runtime, "avg_throughput": avg_throughput})

        print(f"Concurrency {concurrency} - Runtime: {avg_runtime:.2f}s, Throughput: {avg_throughput:.2f} rows/s")

    return results


def plot_results(results, bucket_name, output_key_prefix):
    # Plot scale test results
    scale_test_results = results["scale_test"]
    concurrencies = [res["concurrency"] for res in scale_test_results]
    runtimes = [res["avg_runtime"] for res in scale_test_results]
    throughputs = [res["avg_throughput"] for res in scale_test_results]

    plt.figure(figsize=(12, 6))
    plt.subplot(1, 2, 1)
    plt.plot(concurrencies, runtimes, marker='o')
    plt.xlabel('Concurrency Level')
    plt.ylabel('Average Runtime (seconds)')
    plt.title('Average Runtime vs Concurrency')
    plt.grid()

    plt.subplot(1, 2, 2)
    plt.plot(concurrencies, throughputs, marker='o')
    plt.xlabel('Concurrency Level')
    plt.ylabel('Average Throughput (rows/second)')
    plt.title('Average Throughput vs Concurrency')
    plt.grid()

    # Save plot to S3
    img_data = io.BytesIO()
    plt.savefig(img_data, format='png')
    img_data.seek(0)

    s3_client = boto3.client('s3')
    s3_client.put_object(
        Bucket=bucket_name,
        Key=f"{output_key_prefix}/scale_test_results.png",
        Body=img_data,
        ContentType='image/png'
    )
    print(f"Plot uploaded to S3: s3://{bucket_name}/{output_key_prefix}/scale_test_results.png")


def lambda_handler(event, context):
    presigned_urls = PRESIGNED_URLS
    concurrency_levels = event.get("concurrency_levels", [1, 5, 10, 20])
    s3_bucket = event["s3_bucket"]
    output_key_prefix = event["output_key_prefix"]

    results = throughput_test(presigned_urls, concurrency_levels)
    plot_results(results, s3_bucket, output_key_prefix)

    return {
        "statusCode": 200,
        "message": "Performance metrics calculated and plots uploaded to S3."
    }
