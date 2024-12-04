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
        FunctionName=LAMBDA_FUNCTION_NAME,
        InvocationType='RequestResponse'
    )
    end_time = time.time()
    duration = end_time - start_time
    return duration

# Function to test scalability with an increasing number of invocations
def scalability_test(max_concurrency=100):
    results = []
    for concurrency in range(1, max_concurrency + 1):
        durations = []
        print(f"Testing with concurrency level: {concurrency}")
        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = [executor.submit(invoke_lambda) for _ in range(concurrency)]
            for future in concurrent.futures.as_completed(futures):
                durations.append(future.result())
        avg_duration = sum(durations) / len(durations)
        results.append((concurrency, avg_duration))
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