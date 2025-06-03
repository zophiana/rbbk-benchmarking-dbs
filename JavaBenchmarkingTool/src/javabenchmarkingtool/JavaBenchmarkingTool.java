package javabenchmarkingtool;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class JavaBenchmarkingTool {
    private static final String DB_URL = "jdbc:hsqldb:hsql://localhost/";
    private static final String DB_USER = "SA";
    private static final String DB_PASSWORD = "";
    
    private static final SimpleDateFormat DATE_FORMAT = 
        new SimpleDateFormat("MM/dd/yyyy");
    private static final SimpleDateFormat TIME_FORMAT = 
        new SimpleDateFormat("HH:mm");

    public static void main(String[] args) {
        String tsvFilePath = "../raw-data/Motor_Vehicle_Collisions_100_000.tsv"; // Updated to reflect TSV format
        
        try {
            Class.forName("org.hsqldb.jdbcDriver");
            
            try (Connection connection = DriverManager.getConnection(
                DB_URL, DB_USER, DB_PASSWORD)) {
                
                createTable(connection);
                importTsvData(connection, tsvFilePath);
                
                System.out.println("TSV data import completed successfully!");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void createTable(Connection connection) throws SQLException {
        String createTableSQL = """
            CREATE TABLE IF NOT EXISTS crash_data (
                id BIGINT PRIMARY KEY,
                crash_date DATE,
                crash_time TIME,
                borough VARCHAR(50),
                zip_code VARCHAR(10),
                latitude DECIMAL(10, 6),
                longitude DECIMAL(10, 6),
                location VARCHAR(100),
                on_street_name VARCHAR(100),
                cross_street_name VARCHAR(100),
                off_street_name VARCHAR(100),
                persons_injured INTEGER DEFAULT 0,
                persons_killed INTEGER DEFAULT 0,
                pedestrians_injured INTEGER DEFAULT 0,
                pedestrians_killed INTEGER DEFAULT 0,
                cyclists_injured INTEGER DEFAULT 0,
                cyclists_killed INTEGER DEFAULT 0,
                motorists_injured INTEGER DEFAULT 0,
                motorists_killed INTEGER DEFAULT 0,
                contributing_factor_1 VARCHAR(100),
                contributing_factor_2 VARCHAR(100),
                contributing_factor_3 VARCHAR(100),
                contributing_factor_4 VARCHAR(100),
                contributing_factor_5 VARCHAR(100),
                vehicle_type_1 VARCHAR(50),
                vehicle_type_2 VARCHAR(50),
                vehicle_type_3 VARCHAR(50),
                vehicle_type_4 VARCHAR(50),
                vehicle_type_5 VARCHAR(50)
            )
            """;
        
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(createTableSQL);
            System.out.println("Table created successfully!");
        }
    }

    private static void importTsvData(Connection connection, String tsvFilePath)
            throws IOException, SQLException {

        String insertSQL = """
            INSERT INTO crash_data (
                id, crash_date, crash_time, borough, zip_code, latitude, longitude,
                location, on_street_name, cross_street_name, off_street_name,
                persons_injured, persons_killed, pedestrians_injured, 
                pedestrians_killed, cyclists_injured, cyclists_killed,
                motorists_injured, motorists_killed, contributing_factor_1,
                contributing_factor_2, contributing_factor_3, 
                contributing_factor_4, contributing_factor_5,
                vehicle_type_1, vehicle_type_2, vehicle_type_3, 
                vehicle_type_4, vehicle_type_5
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                     ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;
        try (BufferedReader reader = new BufferedReader(new FileReader(tsvFilePath));
             PreparedStatement pstmt = connection.prepareStatement(insertSQL)) {
            
            // Skip header row
            String headerLine = reader.readLine();
            System.out.println("TSV Header: " + headerLine);
            
            String line;
            int rowCount = 0;
            int errorCount = 0;
            
            while ((line = reader.readLine()) != null) {
                String[] fields = parseTsvLine(line);
                
                if (fields.length >= 29) {
                    try {
                        setStatementParameters(pstmt, fields);
                        pstmt.executeUpdate();
                        rowCount++;
                        
                        if (rowCount % 100 == 0) {
                            System.out.println("Processed " + rowCount + " rows");
                        }
                    } catch (Exception e) {
                        errorCount++;
                        System.err.println("Error processing row " + (rowCount + errorCount) +
                                         ": " + e.getMessage());
                        if (errorCount <= 5) { // Only show first 5 errors to avoid spam
                            System.err.println("Row data: " + line);
                        }
                    }
                } else {
                    errorCount++;
                    System.err.println("Row " + (rowCount + errorCount) +
                                     " has insufficient columns (" + fields.length +
                                     "/29): " + line);
                }
            }
            
            System.out.println("Total rows imported: " + rowCount);
            if (errorCount > 0) {
                System.out.println("Total errors encountered: " + errorCount);
            }
        }
    }

        private static void setStatementParameters(PreparedStatement pstmt,
                                             String[] fields) throws SQLException {
        // Collision ID (moved to first parameter)
        pstmt.setObject(1, parseLong(fields[23])); // id

        // Date and Time
        pstmt.setDate(2, parseDate(fields[0]));
        pstmt.setTime(3, parseTime(fields[1]));

        // Location data
        pstmt.setString(4, nullIfEmpty(fields[2])); // borough
        pstmt.setString(5, nullIfEmpty(fields[3])); // zip_code
        pstmt.setObject(6, parseDouble(fields[4])); // latitude
        pstmt.setObject(7, parseDouble(fields[5])); // longitude
        pstmt.setString(8, nullIfEmpty(fields[6])); // location
        pstmt.setString(9, nullIfEmpty(fields[7])); // on_street_name
        pstmt.setString(10, nullIfEmpty(fields[8])); // cross_street_name
        pstmt.setString(11, nullIfEmpty(fields[9])); // off_street_name

        // Injury/Death counts
        pstmt.setInt(12, parseInt(fields[10])); // persons_injured
        pstmt.setInt(13, parseInt(fields[11])); // persons_killed
        pstmt.setInt(14, parseInt(fields[12])); // pedestrians_injured
        pstmt.setInt(15, parseInt(fields[13])); // pedestrians_killed
        pstmt.setInt(16, parseInt(fields[14])); // cyclists_injured
        pstmt.setInt(17, parseInt(fields[15])); // cyclists_killed
        pstmt.setInt(18, parseInt(fields[16])); // motorists_injured
        pstmt.setInt(19, parseInt(fields[17])); // motorists_killed

        // Contributing factors
        pstmt.setString(20, nullIfEmpty(fields[18])); // contributing_factor_1
        pstmt.setString(21, nullIfEmpty(fields[19])); // contributing_factor_2
        pstmt.setString(22, nullIfEmpty(fields[20])); // contributing_factor_3
        pstmt.setString(23, nullIfEmpty(fields[21])); // contributing_factor_4
        pstmt.setString(24, nullIfEmpty(fields[22])); // contributing_factor_5

        // Vehicle types
        pstmt.setString(25, nullIfEmpty(fields[24])); // vehicle_type_1
        pstmt.setString(26, nullIfEmpty(fields[25])); // vehicle_type_2
        pstmt.setString(27, nullIfEmpty(fields[26])); // vehicle_type_3
        pstmt.setString(28, nullIfEmpty(fields[27])); // vehicle_type_4
        pstmt.setString(29, nullIfEmpty(fields[28])); // vehicle_type_5
    }

    /**
     * Parses a TSV (Tab-Separated Values) line.
     * Handles basic cases where fields are separated by tabs.
     * For more complex TSV parsing with quoted fields, consider using a library like OpenCSV.
     */
    private static String[] parseTsvLine(String line) {
        if (line == null) {
            return new String[0];
        }

        // Split on tabs and handle empty fields
        String[] fields = line.split("\t", -1); // -1 to preserve trailing empty fields

        // Trim whitespace from each field
        for (int i = 0; i < fields.length; i++) {
            if (fields[i] != null) {
                fields[i] = fields[i].trim();
            }
        }

        return fields;
    }

    private static String nullIfEmpty(String value) {
        return (value == null || value.trim().isEmpty()) ? null : value.trim();
    }

    private static Date parseDate(String dateStr) {
        if (dateStr == null || dateStr.trim().isEmpty()) {
            return null;
        }
        try {
            return new Date(DATE_FORMAT.parse(dateStr.trim()).getTime());
        } catch (ParseException e) {
            System.err.println("Error parsing date: " + dateStr);
            return null;
        }
    }

    private static Time parseTime(String timeStr) {
        if (timeStr == null || timeStr.trim().isEmpty()) {
            return null;
        }
        try {
            return new Time(TIME_FORMAT.parse(timeStr.trim()).getTime());
        } catch (ParseException e) {
            System.err.println("Error parsing time: " + timeStr);
            return null;
        }
    }

    private static Double parseDouble(String value) {
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        try {
            return Double.parseDouble(value.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static Long parseLong(String value) {
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static int parseInt(String value) {
        if (value == null || value.trim().isEmpty()) {
            return 0;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            return 0;
        }
    }
}
