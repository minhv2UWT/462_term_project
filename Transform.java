package lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import saaf.Inspector;
import saaf.Response;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class Transform implements RequestHandler<Request, HashMap<String, Object>> {

    @Override
    public HashMap<String, Object> handleRequest(Request request, Context context) {

        // Collect initial data.
        Inspector inspector = new Inspector();
        inspector.inspectAll();

        LambdaLogger logger = context.getLogger();

        // Get parameters from the request.
        int maxRowsToRead = request.getRow();  // Max rows to read from S3
        String bucketName = request.getBucketname();
        String inputFileName = request.getFilename();
        String outputFileName = "transformed_trips.csv";

        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();

        try {
            // Get the input CSV file from S3 bucket.
            S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketName, inputFileName));
            InputStream objectData = s3Object.getObjectContent();

            BufferedReader reader = new BufferedReader(new InputStreamReader(objectData));
            StringBuilder transformedData = new StringBuilder();

            // Process the header row.
            String headerLine = reader.readLine();
            if (headerLine == null) {
                throw new RuntimeException("The CSV file is empty.");
            }

            String[] headers = headerLine.split(",");
            for (int i = 0; i < headers.length; i++) {
                headers[i] = headers[i].trim().toLowerCase().replace("_", " ");
            }

            // Add the header row to the transformed data.
            transformedData.append(String.join(",", headers));
            transformedData.append("\n");

            // Initialize parallel processing.
            Set<String> seenPULocationIDs = ConcurrentHashMap.newKeySet();
            List<Double> numericValues = Collections.synchronizedList(new ArrayList<>());
            int rowCount = 0;

            // Use ExecutorService for parallel processing of rows.
            ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
            List<Callable<Void>> tasks = new ArrayList<>();

            String line;
            while ((line = reader.readLine()) != null && rowCount < maxRowsToRead) {
                final String rowLine = line;  // Capture line for lambda expression

                tasks.add(() -> {
                    String[] columns = rowLine.split(",");
                    String pulocationID = columns[7].trim();  // PULocationID is typically in column 7.
                    
                    // Filter duplicates based on PULocationID
                    if (!seenPULocationIDs.contains(pulocationID)) {
                        seenPULocationIDs.add(pulocationID);

                        synchronized (transformedData) {
                            transformedData.append(String.join(",", Arrays.asList(columns))).append("\n");
                        }

                        // Collect numeric values
                        for (String column : columns) {
                            try {
                                numericValues.add(Double.parseDouble(column.trim()));
                            } catch (NumberFormatException ignored) {
                            }
                        }
                    }
                    return null;
                });

                rowCount++;
            }

            // Execute all tasks
            executorService.invokeAll(tasks);
            executorService.shutdown();

            reader.close();

            // Calculate the median efficiently without full sorting.
            double median = calculateMedian(numericValues);

            logger.log("Calculated median: " + median);

            // Upload the transformed data to S3.
            byte[] bytes = transformedData.toString().getBytes(StandardCharsets.UTF_8);
            InputStream transformedInputStream = new ByteArrayInputStream(bytes);
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(bytes.length);
            metadata.setContentType("text/csv");

            s3Client.putObject(bucketName, outputFileName, transformedInputStream, metadata);

            logger.log("Transformed file uploaded to S3 bucket: " + bucketName + " as " + outputFileName);

            // Return success response with row count and median
            Response response = new Response();
            response.setValue("Transformed file created: " + outputFileName + ". Processed rows: " + rowCount + ", Median: " + median);

            inspector.consumeResponse(response);
            inspector.inspectAllDeltas();
            return inspector.finish();

        } catch (Exception e) {
            logger.log("Error processing file: " + e.getMessage());
            e.printStackTrace();
            Response response = new Response();
            response.setValue("Error: " + e.getMessage());

            inspector.consumeResponse(response);
            inspector.inspectAllDeltas();
            return inspector.finish();
        }
    }

    // Method to calculate median without full sorting
    private double calculateMedian(List<Double> numericValues) {
        if (numericValues.isEmpty()) return 0.0;

        Collections.sort(numericValues);
        int size = numericValues.size();
        if (size % 2 == 0) {
            return (numericValues.get(size / 2 - 1) + numericValues.get(size / 2)) / 2.0;
        } else {
            return numericValues.get(size / 2);
        }
    }
}
