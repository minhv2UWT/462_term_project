package lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import saaf.Inspector;
import saaf.Response;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.HashMap;
import java.util.List;

public class Query implements RequestHandler<Request, HashMap<String, Object>> {

    private static final String LOCAL_DB_PATH = "/tmp/transformed_trips.db";
    private static final String S3_DB_PATH = "transformed_trips.db";

    @Override
    public HashMap<String, Object> handleRequest(Request request, Context context) {

        // Collect initial data for inspection (optional)
        Inspector inspector = new Inspector();
        inspector.inspectAll();

        // Get request parameters
        String bucketname = request.getBucketname();
        String filename = request.getFilename();
        List<String> groupByColumns = request.getGroupByColumns();  // List of columns to GROUP BY
        String whereClause = request.getWhereClause();  // Optional WHERE clause
        String aggregationFunction = request.getAggregationFunction();  // Aggregation function (e.g., SUM, AVG)

        LambdaLogger logger = context.getLogger();

        // Log the contents of /tmp directory before any actions
        File tmpDir = new File("/tmp");
        File[] files = tmpDir.listFiles();

        if (files != null && files.length > 0) {
            for (File file : files) {
                logger.log("File in /tmp: " + file.getName());
            }
        } else {
            logger.log("No files found in /tmp.");
        }

        // Ensure the database file is available locally
        File dbFile = new File(LOCAL_DB_PATH);
        
        // Log if the database exists before deletion
        if (dbFile.exists()) {
            logger.log("Old database exists, attempting to delete...");
            boolean deleted = dbFile.delete();
            if (deleted) {
                logger.log("Deleted old local database file.");
            } else {
                logger.log("Failed to delete old local database file.");
            }
        } else {
            logger.log("No old database file to delete.");
        }

        // Explicitly delete the file before downloading the new one
        try {
            if (Files.exists(Paths.get(LOCAL_DB_PATH))) {
                Files.delete(Paths.get(LOCAL_DB_PATH));  // Delete the file explicitly
                logger.log("Explicitly deleted the old database file.");
            }
        } catch (IOException e) {
            logger.log("Error deleting the old database file: " + e.getMessage());
            e.printStackTrace();
        }

        // Download the latest database from S3
        try {
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
            S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketname, S3_DB_PATH));
            InputStream objectData = s3Object.getObjectContent();

            // Save the S3 object to the local file system
            Files.copy(objectData, Paths.get(LOCAL_DB_PATH));
            logger.log("Downloaded database from S3: " + S3_DB_PATH);
        } catch (Exception e) {
            logger.log("Error downloading the database from S3: " + e.getMessage());
            e.printStackTrace();
            return null;
        }

        // Verify that the new database file exists
        File newDbFile = new File(LOCAL_DB_PATH);
        logger.log("New database file exists: " + newDbFile.exists());

        // Connect to the SQLite database and query
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + LOCAL_DB_PATH)) {
            // Build the SQL query dynamically
            StringBuilder sql = new StringBuilder("SELECT ");
            
            // Add aggregation function to the first column in groupByColumns
            String aggregatedColumn = groupByColumns.get(0); // First column for aggregation
            sql.append(aggregationFunction).append("(").append(aggregatedColumn).append(") AS aggregated_value, ");
            
            // Add all groupByColumns to the SELECT clause
            sql.append(String.join(", ", groupByColumns));
            sql.append(" FROM trips");

            // Add WHERE clause if provided
            if (whereClause != null && !whereClause.isEmpty()) {
                sql.append(" WHERE ").append(whereClause);
            }

            // Add GROUP BY clause based on groupByColumns
            sql.append(" GROUP BY ").append(String.join(", ", groupByColumns));

            // Prepare and execute the query
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql.toString());

            // Collect query results dynamically based on the columns in the result set
            StringBuilder result = new StringBuilder();

            // Add header dynamically based on groupByColumns
            for (String column : groupByColumns) {
                result.append(String.format("%-15s", column));
            }
            result.append("Aggregated Value");
            result.append("\n----------------------------------------------------\n");

            // Process each row
            while (rs.next()) {
                for (String column : groupByColumns) {
                    result.append(String.format("%-15s", rs.getString(column)));
                }
                result.append(rs.getString("aggregated_value"));
                result.append("\n");
            }

            // Log the result
            logger.log("Query result:\n" + result.toString());

            // Create a response object
            Response response = new Response();
            response.setValue(result.toString());

            inspector.consumeResponse(response);
        } catch (SQLException e) {
            logger.log("Error querying the database: " + e.getMessage());
            e.printStackTrace();
            return null;
        }

        // Collect final inspection data (optional)
        inspector.inspectAllDeltas();
        return inspector.finish();
    }
}
