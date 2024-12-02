package lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import saaf.Inspector;
import saaf.Response;
import java.util.HashMap;

/**
 * uwt.lambda_test::handleRequest
 *
 * @author Wes Lloyd
 * @author Robert Cordingly
 */
public class CreateCSV implements RequestHandler<Request, HashMap<String, Object>> {

    /**
     * Lambda Function Handler
     * 
     * @param request Request POJO with defined variables from Request.java
     * @param context 
     * @return HashMap that Lambda will automatically convert into JSON.
     */
    public HashMap<String, Object> handleRequest(Request request, Context context) {

        // Collect initial data
        Inspector inspector = new Inspector();
        inspector.inspectAll();

        // Retrieve request parameters
        String bucketname = request.getBucketname();
        String filename = request.getFilename();
        String csvFilePath = "/taxi_zone_lookup.csv";


        try {
            // Read the CSV file from the local filesystem
            File csvFile = new File(csvFilePath);
            if (!csvFile.exists()) {
                throw new RuntimeException("File not found: " + csvFilePath);
            }

            // Prepare the file as InputStream
            InputStream is = new FileInputStream(csvFile);

            // Set up metadata
            ObjectMetadata meta = new ObjectMetadata();
            meta.setContentLength(csvFile.length());
            meta.setContentType("text/csv");

            // Upload the file to S3
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
            s3Client.putObject(bucketname, filename, is, meta);

            // Response message
            Response response = new Response();
            response.setValue("Bucket: " + bucketname + ", Filename: " + filename + ", Size: " + csvFile.length());
            inspector.consumeResponse(response);

        } catch (Exception e) {
            // Handle exceptions
            context.getLogger().log("Error uploading file to S3: " + e.getMessage());
            Response response = new Response();
            response.setValue("Failed to upload file: " + e.getMessage());
            inspector.consumeResponse(response);
        }

        // Collect final information such as runtime and CPU deltas
        inspector.inspectAllDeltas();
        return inspector.finish();
    }
}
