package javabenchmarkingtool;

import java.io.IOException;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.*;

public class QueryDatabase {

    private static final Logger LOGGER = Logger.getLogger(QueryDatabase.class.getName());
    String LOG_FILE_PATH = "/home/info/Dokumente/query.log";

    public String getCurrentDate() {
        SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy h:mm:ss.SSS a");
        return sdf.format(new Date());
    }

    public void executeQuery(String query, String driver, String url, String u, String p) throws ClassNotFoundException, IOException {
        setupLogger();

        Class.forName(driver);

        String start = getCurrentDate();
        LOGGER.log(Level.INFO, "Query started: {0}", start);

        try (Connection conn = DriverManager.getConnection(url, u, p); PreparedStatement stmt = conn.prepareStatement(query); ResultSet rs = stmt.executeQuery()) {
            String end = getCurrentDate();
            LOGGER.log(Level.INFO, "Query finished: {0}", end);
            int i = 0;
            while (rs.next()) {
                i = i + 1;
            }
            logExecutionTime(start, end);
            LOGGER.log(Level.INFO, "Retrieved {0} Rows", i);
            LOGGER.log(Level.INFO, "For Query: \"{0}\"", query);

        } catch (SQLException e) {
            LOGGER.log(Level.SEVERE, "Error executing query", e);
        }
    }

    private void setupLogger() throws IOException {
        FileHandler fh = new FileHandler(LOG_FILE_PATH, true); // Append to the log file
        LOGGER.setUseParentHandlers(false);
        fh.setFormatter(new SimpleFormatter());
        LOGGER.addHandler(fh);
    }

    private void logExecutionTime(String start, String end) {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy h:mm:ss.SSS a");
            Date startDate = sdf.parse(start);
            Date endDate = sdf.parse(end);

            long diffMillis = endDate.getTime() - startDate.getTime();
            LOGGER.log(Level.INFO, "Execution time: {0}ms", diffMillis);

        } catch (ParseException e) {
            LOGGER.log(Level.SEVERE, "Error calculating execution time", e);
        }
    }
}
