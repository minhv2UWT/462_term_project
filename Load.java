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
import java.sql.*;
import java.util.HashMap;
import java.util.Scanner;

public class Load implements RequestHandler<Request, HashMap<String, Object>> {

    @Override
    public HashMap<String, Object> handleRequest(Request request, Context context) {

        // Collect initial data
        Inspector inspector = new Inspector();
        inspector.inspectAll();

        // Get parameters from the request
        String bucketname = request.getBucketname();
        String inputFileName = request.getFilename();
        String dbFileName = "/tmp/transformed_trips.db";  // SQLite database file location in /tmp
        LambdaLogger logger = context.getLogger();

        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();

        try {
            // Get the input CSV file from S3 bucket
            S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketname, inputFileName));
            InputStream objectData = s3Object.getObjectContent();

            // Create SQLite database connection
            Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbFileName);
            Statement stmt = conn.createStatement();

            // Disable auto-commit mode
            conn.setAutoCommit(false);

            // Create the trips table in SQLite
            String createTableSQL = "CREATE TABLE IF NOT EXISTS trips (" +
                    "hvfhs_license_num TEXT, " +
                    "dispatching_base_num TEXT, " +
                    "originating_base_num TEXT, " +
                    "request_datetime TEXT, " +
                    "on_scene_datetime TEXT, " +
                    "pickup_datetime TEXT, " +
                    "dropoff_datetime TEXT, " +
                    "PULocationID INTEGER, " +
                    "DOLocationID INTEGER, " +
                    "trip_miles REAL, " +
                    "trip_time INTEGER, " +
                    "base_passenger_fare REAL, " +
                    "tolls REAL, " +
                    "bcf REAL, " +
                    "sales_tax REAL, " +
                    "congestion_surcharge REAL, " +
                    "airport_fee REAL, " +
                    "tips REAL, " +
                    "driver_pay REAL, " +
                    "shared_request_flag TEXT, " +
                    "shared_match_flag TEXT, " +
                    "access_a_ride_flag TEXT, " +
                    "wav_request_flag TEXT, " +
                    "wav_match_flag TEXT" +
                    ");";
            stmt.execute(createTableSQL);

            // Read CSV and insert data into SQLite table
            Scanner scanner = new Scanner(objectData);
            while (scanner.hasNext()) {
                String line = scanner.nextLine();
                String[] values = line.split(",");

                // Ensure you are inserting all 24 columns
                String insertSQL = "INSERT INTO trips (" +
                        "hvfhs_license_num, dispatching_base_num, originating_base_num, request_datetime, " +
                        "on_scene_datetime, pickup_datetime, dropoff_datetime, PULocationID, DOLocationID, trip_miles, " +
                        "trip_time, base_passenger_fare, tolls, bcf, sales_tax, congestion_surcharge, airport_fee, " +
                        "tips, driver_pay, shared_request_flag, shared_match_flag, access_a_ride_flag, wav_request_flag, wav_match_flag) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                
                try (PreparedStatement pstmt = conn.prepareStatement(insertSQL)) {
                    for (int i = 0; i < values.length; i++) {
                        pstmt.setString(i + 1, values[i].trim());
                    }
                    pstmt.executeUpdate();
                }
            }
            scanner.close();

            // Commit the transaction manually
            conn.commit();

            // Close SQLite connection
            stmt.close();
            conn.close();

            // Upload the SQLite database to S3
            File dbFile = new File(dbFileName);
            try (InputStream dbInputStream = new FileInputStream(dbFile)) {
                ObjectMetadata meta = new ObjectMetadata();
                meta.setContentLength(dbFile.length());
                meta.setContentType("application/x-sqlite3");

                s3Client.putObject(bucketname, "transformed_trips.db", dbInputStream, meta);
                logger.log("Database file uploaded to S3: " + bucketname + "/transformed_trips.db");
            }

        } catch (Exception e) {
            logger.log("Error processing file: " + e.getMessage());
            e.printStackTrace();
        }

        // Create and populate response object for function output
        Response response = new Response();
        response.setValue("SQLite Database file created and uploaded: transformed_trips.db");

        inspector.consumeResponse(response);

        // Collect final information such as total runtime and cpu deltas.
        inspector.inspectAllDeltas();
        return inspector.finish();
    }
}