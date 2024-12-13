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

public class Transform implements RequestHandler<Request, HashMap<String, Object>> {

    @Override
    public HashMap<String, Object> handleRequest(Request request, Context context) {

        // Collect initial data.
        Inspector inspector = new Inspector();
        inspector.inspectAll();

        LambdaLogger logger = context.getLogger();

        // Get parameters from the request.
        int maxRows = request.getRow();
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

            // Process each row and filter out duplicates based on 'PULocationID'.
            String line;
            Set<String> seenPULocationIDs = new HashSet<>();
            int rowCount = 0;
            List<Double> numericValues = new ArrayList<>();

            while ((line = reader.readLine()) != null && rowCount < maxRows) {
                String[] columns = line.split(",");

                // Filter duplicates based on PULocationID (assuming PULocationID is in columns[7]).
                String pulocationID = columns[7].trim();  // PULocationID is typically in column 7.
                if (seenPULocationIDs.contains(pulocationID)) {
                    continue;  // Skip this row if it's a duplicate.
                }

                seenPULocationIDs.add(pulocationID);

                StringBuilder row = new StringBuilder();
                for (int i = 0; i < columns.length; i++) {
                    row.append(columns[i].trim());
                    if (i < columns.length - 1) {
                        row.append(",");
                    }

                    try {
                        numericValues.add(Double.parseDouble(columns[i].trim()));
                    } catch (NumberFormatException ignored) {
                    }
                }

                transformedData.append(row).append("\n");
                rowCount++;
            }

            reader.close();

            // Calculate the median of numeric values.
            Collections.sort(numericValues);
            double median = numericValues.size() % 2 == 0
                    ? (numericValues.get(numericValues.size() / 2 - 1) + numericValues.get(numericValues.size() / 2)) / 2.0
                    : numericValues.get(numericValues.size() / 2);

            logger.log("Calculated median: " + median);

            // Upload the transformed data to S3.
            byte[] bytes = transformedData.toString().getBytes(StandardCharsets.UTF_8);
            InputStream transformedInputStream = new ByteArrayInputStream(bytes);
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(bytes.length);
            metadata.setContentType("text/csv");

            s3Client.putObject(bucketName, outputFileName, transformedInputStream, metadata);

            logger.log("Transformed file uploaded to S3 bucket: " + bucketName + " as " + outputFileName);

        } catch (Exception e) {
            logger.log("Error processing file: " + e.getMessage());
            e.printStackTrace();
        }

        // Return a response indicating that the file has been created.
        Response response = new Response();
        response.setValue("Transformed file created: " + outputFileName);

        inspector.consumeResponse(response);
        inspector.inspectAllDeltas();
        return inspector.finish();
    }
}
