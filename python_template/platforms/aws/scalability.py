import boto3
import json
import time
import concurrent.futures
import matplotlib.pyplot as plt

# Initialize Boto3 client for invoking the Lambda function
lambda_client = boto3.client('lambda')

# Define your Lambda function details
LAMBDA_FUNCTION_NAME = "your_lambda_function_name"

# Pre-signed URLs for the CSV files
CSV_FILES = [
    "https://462-proj-dataset.s3.us-east-1.amazonaws.com/fhvhv_tripdata_2021-01.csv?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAQ3EGTNWWJQM6KGE7%2F20241212%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20241212T194532Z&X-Amz-Expires=604800&X-Amz-SignedHeaders=host&X-Amz-Signature=041ef60d7f058bd9585315cc054457ea750976e543bb81be1c0c68e79f7bbd57",
    "https://462-proj-dataset.s3.us-east-1.amazonaws.com/fhvhv_tripdata_2021-02.csv?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAQ3EGTNWWJQM6KGE7%2F20241212%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20241212T194532Z&X-Amz-Expires=604800&X-Amz-SignedHeaders=host&X-Amz-Signature=2c00c0387114fde5a9c2e02d8c8c0c6e00be56fa942b86d0521c70fa1c53243e",
    "https://462-proj-dataset.s3.us-east-1.amazonaws.com/fhvhv_tripdata_2021-03.csv?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAQ3EGTNWWJQM6KGE7%2F20241212%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20241212T194532Z&X-Amz-Expires=604800&X-Amz-SignedHeaders=host&X-Amz-Signature=78d40729cd3116c1dc8e81365852f858e352d19d4c134c675136437acc51b2c0",
]

# Function to invoke the Lambda function and measure execution time
def invoke_lambda(file_url):
    start_time = time.time()
    response = lambda_client.invoke(
        FunctionName=LAMBDA_FUNCTION_NAME,
        InvocationType='RequestResponse',
        Payload=json.dumps({"file_url": file_url})
    )
    end_time = time.time()
    duration = end_time - start_time
    return duration

# Function to test scalability with an increasing number of invocations
def scalability_test(csv_files, max_concurrency=100):
    results = []
    for concurrency in range(1, max_concurrency + 1):
        durations = []
        print(f"Testing with concurrency level: {concurrency}")
        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = [
                executor.submit(invoke_lambda, csv_files[i % len(csv_files)])
                for i in range(concurrency)
            ]
            for future in concurrent.futures.as_completed(futures):
                durations.append(future.result())
        avg_duration = sum(durations) / len(durations)  # average runtime
        results.append((concurrency, avg_duration))  # store concurrency level & avg duration
        print(f"Average runtime for concurrency {concurrency}: {avg_duration:.2f} seconds")
    return results

# Function to visualize the scalability test results
def plot_results(results):
    concurrencies = [result[0] for result in results]
    avg_durations = [result[1] for result in results]
    plt.figure(figsize=(10, 6))
    plt.plot(concurrencies, avg_durations, marker='o')
    plt.xlabel('Concurrency Level')
    plt.ylabel('Average Lambda Runtime (seconds)')
    plt.title('Lambda Function Scalability Test')
    plt.grid(True)
    plt.show()

# Run scalability test and plot results
if __name__ == "__main__":
    test_results = scalability_test(CSV_FILES, max_concurrency=100)
    plot_results(test_results)
