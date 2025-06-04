package javabenchmarkingtool;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class QueryDatabase {

    private final String LOG_FILE_PATH;

    public QueryDatabase(String log_file_path) {
        this.LOG_FILE_PATH = log_file_path;
    }

    public String getCurrentDate() {
        SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy h:mm:ss.SSS a");
        return sdf.format(new Date());
    }

    public void executeQuery(String query, String driver, String url, String u, String p) throws ClassNotFoundException, IOException {
        Class.forName(driver);

        String start = getCurrentDate();
        
        try (FileWriter file = new FileWriter(LOG_FILE_PATH, true)) {
            file.write("INFO: Query started: " + start + "\n");

            try (Connection conn = DriverManager.getConnection(url, u, p); 
                 PreparedStatement stmt = conn.prepareStatement(query); 
                 ResultSet rs = stmt.executeQuery()) {
                
                String end = getCurrentDate();
                file.write("INFO: Query finished: " + end + "\n");
                
                int i = 0;
                while (rs.next()) {
                    i = i + 1;
                }
                
                logExecutionTime(start, end, file);
                file.write("INFO: Retrieved " + i + " Rows\n");
                file.write("INFO: For Query: \"" + query + "\"\n");

            } catch (SQLException e) {
                file.write("SEVERE: Error executing query: " + e.getMessage() + "\n");
                throw new IOException("Database error", e);
            }
        }
    }

    private void logExecutionTime(String start, String end, FileWriter file) throws IOException {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy h:mm:ss.SSS a");
            Date startDate = sdf.parse(start);
            Date endDate = sdf.parse(end);

            long diffMillis = endDate.getTime() - startDate.getTime();
            file.write("INFO: Execution time: " + diffMillis + "ms\n");

        } catch (ParseException e) {
            file.write("SEVERE: Error calculating execution time: " + e.getMessage() + "\n");
        }
    }
}
