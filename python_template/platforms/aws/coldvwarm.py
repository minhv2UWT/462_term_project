import boto3
import time
import requests
import json
import csv

# Initialize AWS Lambda client
lambda_client = boto3.client('lambda')

# Pre-signed URLs for the CSV files
PRESIGNED_URLS = [
    "https://462-proj-dataset.s3.us-east-1.amazonaws.com/fhvhv_tripdata_2021-01.csv?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAQ3EGTNWWJQM6KGE7%2F20241212%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20241212T194532Z&X-Amz-Expires=604800&X-Amz-SignedHeaders=host&X-Amz-Signature=041ef60d7f058bd9585315cc054457ea750976e543bb81be1c0c68e79f7bbd57",
    "https://462-proj-dataset.s3.us-east-1.amazonaws.com/fhvhv_tripdata_2021-02.csv?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAQ3EGTNWWJQM6KGE7%2F20241212%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20241212T194532Z&X-Amz-Expires=604800&X-Amz-SignedHeaders=host&X-Amz-Signature=2c00c0387114fde5a9c2e02d8c8c0c6e00be56fa942b86d0521c70fa1c53243e",
    "https://462-proj-dataset.s3.us-east-1.amazonaws.com/fhvhv_tripdata_2021-03.csv?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAQ3EGTNWWJQM6KGE7%2F20241212%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20241212T194532Z&X-Amz-Expires=604800&X-Amz-SignedHeaders=host&X-Amz-Signature=78d40729cd3116c1dc8e81365852f858e352d19d4c134c675136437acc51b2c0",
]


def invoke_lambda(url, cold_start=False):
    """Invoke AWS Lambda with the given S3 URL."""
    start_time = time.time()
    response = lambda_client.invoke(
        FunctionName="your_lambda_function_name",
        InvocationType='RequestResponse',
        Payload=json.dumps({"url": url})
    )
    end_time = time.time()

    # Process Lambda response
    payload = json.loads(response['Payload'].read())
    rows_processed = payload.get("rows_processed", 0)
    execution_time = payload.get("execution_time", 0)  # Assuming Lambda returns this
    init_time = payload.get("init_time", 0) if cold_start else 0

    total_time = end_time - start_time
    throughput = rows_processed / total_time if total_time > 0 else 0

    return {
        "execution_time": execution_time,
        "init_time": init_time,
        "total_time": total_time,
        "throughput": throughput,
        "rows_processed": rows_processed
    }


def cold_warm_test(presigned_urls, iterations=5):
    """Perform cold and warm tests for AWS Lambda."""
    results = {
        "cold": [],
        "warm": []
    }

    # Cold start test
    print("Performing cold start test...")
    for url in presigned_urls:
        res = invoke_lambda(url, cold_start=True)
        results["cold"].append(res)

    # Warm start test
    print("Performing warm start test...")
    for _ in range(iterations):
        for url in presigned_urls:
            res = invoke_lambda(url, cold_start=False)
            results["warm"].append(res)

    return results


def summarize_results(results):
    """Summarize the results for cold and warm tests."""
    cold_metrics = {
        "average_execution_time": sum(r["execution_time"] for r in results["cold"]) / len(results["cold"]),
        "average_init_time": sum(r["init_time"] for r in results["cold"]) / len(results["cold"]),
        "average_throughput": sum(r["throughput"] for r in results["cold"]) / len(results["cold"]),
    }

    warm_metrics = {
        "average_execution_time": sum(r["execution_time"] for r in results["warm"]) / len(results["warm"]),
        "average_throughput": sum(r["throughput"] for r in results["warm"]) / len(results["warm"]),
    }

    print("\nCold Performance Metrics:")
    print(f"  Average Execution Time: {cold_metrics['average_execution_time']:.2f} seconds")
    print(f"  Average Init Time: {cold_metrics['average_init_time']:.2f} seconds")
    print(f"  Average Throughput: {cold_metrics['average_throughput']:.2f} rows/second")

    print("\nWarm Performance Metrics:")
    print(f"  Average Execution Time: {warm_metrics['average_execution_time']:.2f} seconds")
    print(f"  Average Throughput: {warm_metrics['average_throughput']:.2f} rows/second")

    return {"cold": cold_metrics, "warm": warm_metrics}


def lambda_handler(event, context):
    presigned_urls = PRESIGNED_URLS
    iterations = event.get("iterations", 5)  # Number of warm iterations per file

    # Perform the cold/warm test
    results = cold_warm_test(presigned_urls, iterations)

    # Summarize the results
    metrics = summarize_results(results)

    return {
        "statusCode": 200,
        "cold_metrics": metrics["cold"],
        "warm_metrics": metrics["warm"]
    }


if __name__ == "__main__":
    # Example local test
    test_event = {"iterations": 5}
    lambda_handler(test_event, None)
