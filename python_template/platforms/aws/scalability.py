import boto3
import time
import concurrent.futures
import matplotlib.pyplot as plt

# Initialize Boto3 client for invoking the Lambda function
lambda_client = boto3.client('lambda')

# Define your Lambda function details
LAMBDA_FUNCTION_NAME = "your_lambda_function_name"

# Function to invoke the Lambda function and measure execution time
def invoke_lambda():
    start_time = time.time()
    response = lambda_client.invoke(
        FunctionName=LAMBDA_FUNCTION_NAME, #replace
        InvocationType='RequestResponse'
        Payload=json.dumps({
            "bucket": bucket_name, #replace
            "key": file_key #replace 
    })
    end_time = time.time()
    duration = end_time - start_time
    return duration
PARQUET_FILES = [
    "fhvhv_tripdata_2021-01.parquet",
    "fhvhv_tripdata_2021-02.parquet",
    "fhvhv_tripdata_2021-03.parquet",
    "fhvhv_tripdata_2021-04.parquet",
    "fhvhv_tripdata_2021-05.parquet",
    "fhvhv_tripdata_2021-06.parquet",
    "fhvhv_tripdata_2021-07.parquet",
    "fhvhv_tripdata_2021-08.parquet",
    "fhvhv_tripdata_2021-09.parquet",
    "fhvhv_tripdata_2021-10.parquet",
    "fhvhv_tripdata_2021-11.parquet",
    "fhvhv_tripdata_2021-12.parquet",
]
    
# Function to test scalability with an increasing number of invocations
def scalability_test(bucket_name, parquet_files, max_concurrency=100):
    results = []
    for concurrency in range(1, max_concurrency + 1):
        durations = []
        print(f"Testing with concurrency level: {concurrency}")
        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = [executor.submit(invoke_lambda, bucket_name, parquet_files[i % len(parquet_files)]
            for i in range(concurrency)
        ]
        for future in concurrent.futures.as_completed(futures):
            durations.append(future.result())
        avg_duration = sum(durations) / len(duration) #average runtime
        results.append((concurrency, avg_duration)) #store concurrency level & avg duration 
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
    test_results = scalability_test(max_concurrency=100)
    plot_results(test_results)
