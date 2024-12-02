package lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Scanner;
import saaf.Inspector;

/**
 * Lambda Function for processing CSV files.
 */
public class ProcessCSV implements RequestHandler<Request, HashMap<String, Object>> {

    @Override
    public HashMap<String, Object> handleRequest(Request request, Context context) {
        LambdaLogger logger = context.getLogger();
        Inspector inspector = new Inspector();
        inspector.inspectAll();  // Initial inspection of Lambda environment

        String bucketName = request.getBucketname();
        String fileName = request.getFilename();

        long startTime = System.currentTimeMillis();
        long totalValue = 0;
        long rowCount = 0;
        long elementCount = 0;

        try {
            // Access the S3 bucket
            AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
            S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketName, fileName));
            InputStream inputStream = s3Object.getObjectContent();

            // Process CSV content
            Scanner scanner = new Scanner(inputStream);
            
            // Skip header line if present
            if (scanner.hasNextLine()) {
                String headerLine = scanner.nextLine();
                logger.log("Skipping header: " + headerLine);
            }

            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                String[] values = line.split(",");

                for (String value : values) {
                    // Ensure value is numeric before parsing
                    if (!value.trim().isEmpty() && value.trim().matches("-?\\d+")) {
                        totalValue += Long.parseLong(value.trim());
                        elementCount++;
                    } else {
                        logger.log("Non-numeric value skipped: " + value);
                    }
                }
                rowCount++;
            }
            scanner.close();

            // Calculate performance metrics
            double average = (elementCount > 0) ? (double) totalValue / elementCount : 0;

            long endTime = System.currentTimeMillis();
            long totalExecutionTime = endTime - startTime;

            // Calculate throughput (rows per second)
            double throughput = (rowCount > 0) ? rowCount / (totalExecutionTime / 1000.0) : 0;  // rows per second

            logger.log(String.format("Processed %d rows and %d elements. Total Value: %d. Average: %.2f", 
                rowCount, elementCount, totalValue, average));
            logger.log(String.format("Execution Time: %d ms", totalExecutionTime));
            logger.log(String.format("Throughput: %.2f rows/second", throughput));

            // Store metrics in Inspector
            inspector.addAttribute("rowCount", rowCount);
            inspector.addAttribute("elementCount", elementCount);
            inspector.addAttribute("totalValue", totalValue);
            inspector.addAttribute("average", average);
            inspector.addAttribute("executionTime", totalExecutionTime);
            inspector.addAttribute("throughput", throughput);

            inspector.inspectAllDeltas();  // Final inspection
            return inspector.finish();  // Return all metrics in the Inspector

        } catch (Exception e) {
            logger.log("Error processing CSV file: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
