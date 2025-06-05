package javabenchmarkingtool;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.sql.SQLTimeoutException;
import java.util.*;

/**
 * Utility for benchmarking a set of queries against multiple databases,
 * repeated N times to collect stats including per-query timeout handling.
 */
public class QueryDatabase {

  /**
   * Holder for one DB’s connection info + a name for logs
   */
  public static class DbConfig {

    public final String name, driver, url, user, pass;

    public DbConfig(
            String name,
            String driver,
            String url,
            String user,
            String pass
    ) {
      this.name = name;
      this.driver = driver;
      this.url = url;
      this.user = user;
      this.pass = pass;
    }
  }

  /**
   * Benchmarks each SQL in `queries` against each DB in `configs`, repeating
   * each query `runs` times.
   *
   * @param logFilePath path to append logs to
   * @param queries list of SQL strings
   * @param runs how many times to run each SQL
   * @param configs var-args list of DbConfig
   */
  public static void benchmark(
          String logFilePath,
          List<String> queries,
          int runs,
          DbConfig... configs
  ) throws ClassNotFoundException, IOException {
    // 1) Load all drivers
    for (DbConfig cfg : configs) {
      Class.forName(cfg.driver);
    }

    // 2) For each DB
    for (DbConfig cfg : configs) {
      appendLog(logFilePath,
              "INFO: ===== Benchmarking " + cfg.name + " =====");
      try (Connection conn = DriverManager.getConnection(
              cfg.url, cfg.user, cfg.pass)) {
        // for each query
        for (String sql : queries) {
          runRepeated(conn, logFilePath, cfg.name, sql, runs);
        }
      } catch (SQLException e) {
        appendLog(logFilePath,
                "SEVERE: [" + cfg.name + "] Connection error: "
                + e.getMessage());
      }
    }
  }

  // timeout constants: 300 seconds == 5 minutes
  private static final int TIMEOUT_SEC = 300;
  private static final long TIMEOUT_MS = TIMEOUT_SEC * 1_000L;

  /**
   * Execute one SQL N times under a per-query timeout. Collects durations (ms),
   * counts timeouts, then logs stats.
   */
  private static void runRepeated(
          Connection conn,
          String logFilePath,
          String dbName,
          String query,
          int runs
  ) throws IOException {
    List<Long> times = new ArrayList<>(runs);
    Integer rows = null;

    for (int i = 0; i < runs; i++) {
      try (PreparedStatement ps = conn.prepareStatement(query)) {
        ps.setQueryTimeout(TIMEOUT_SEC);

        long start = System.nanoTime();
        int cnt = 0;

        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            cnt++;
          }
          long took = (System.nanoTime() - start) / 1_000_000;
          times.add(took);
          if (rows == null) {
            rows = cnt;
          }
        } catch (SQLException se) {
          // query timed out
          times.add(TIMEOUT_MS);
          appendLog(logFilePath,
                  "WARNING: [" + dbName + "] Timeout on run "
                  + (i + 1) + " after " + TIMEOUT_SEC + "s");
        }

      } catch (SQLException se) {
        appendLog(logFilePath,
                "SEVERE: [" + dbName + "] Error on run "
                + (i + 1) + ": " + se.getMessage());
        // do NOT abort the benchmark—just skip this run
      }
    }

    Stats stats = new Stats(times, TIMEOUT_MS);
    appendLog(logFilePath,
            "INFO: [" + dbName + "] SQL: \"" + query + "\"");
    appendLog(logFilePath,
            "INFO: [" + dbName + "] Runs: " + runs);
    appendLog(logFilePath,
            "INFO: [" + dbName + "] Timeouts: "
            + stats.timeoutCount);
    appendLog(logFilePath,
            "INFO: [" + dbName + "] Rows returned: "
            + (rows != null ? rows : "N/A"));
    appendLog(logFilePath,
            "INFO: [" + dbName + "] First run: "
            + stats.first + "ms");
    appendLog(logFilePath,
            "INFO: [" + dbName + "] Last run: "
            + stats.last + "ms");
    appendLog(logFilePath,
            "INFO: [" + dbName + "] Min time: "
            + stats.min + "ms");
    appendLog(logFilePath,
            "INFO: [" + dbName + "] Max time: "
            + stats.max + "ms");
    appendLog(logFilePath,
            String.format("INFO: [%s] Avg time: %.2fms",
                    dbName, stats.average));
    appendLog(logFilePath,
            String.format("INFO: [%s] Median: %.2fms\n",
                    dbName, stats.median));
  }

  // append a single line (with newline) to the log
  private static void appendLog(String path, String line)
          throws IOException {
    try (FileWriter fw = new FileWriter(path, true)) {
      fw.write(line + "\n");
    }
  }

  /**
   * Compute first, last, min, max, avg, median, std-dev over a list of
   * durations (in ms), and count timeouts.
   */
  private static class Stats {

    public final long first, last, min, max;
    public final double average, median, stdDev;
    public final int timeoutCount;

    public Stats(List<Long> values, long timeoutThreshold) {
      List<Long> orig = new ArrayList<>(values);
      List<Long> sorted = new ArrayList<>(values);
      Collections.sort(sorted);

      if (!orig.isEmpty() && !sorted.isEmpty()) {

        first = orig.get(0);
        last = orig.get(orig.size() - 1);
        min = sorted.get(0);
        max = sorted.get(sorted.size() - 1);

        long sum = 0;
        for (long v : orig) {
          sum += v;
        }
        average = (double) sum / orig.size();

        int n = sorted.size();
        if (n % 2 == 1) {
          median = sorted.get(n / 2);
        } else {
          median = (sorted.get(n / 2 - 1) + sorted.get(n / 2)) / 2.0;
        }

        double ss = 0;
        for (long v : orig) {
          double d = v - average;
          ss += d * d;
        }
        stdDev = Math.sqrt(ss / orig.size());
      } else {
        first = 0;
        last = 0;
        min = 0;
        max = 0;
        average = 0;
        median = 0;
        stdDev = 0;
      }
      int to = 0;
      for (long v : orig) {
        if (v >= timeoutThreshold) {
          to++;
        }
      }
      timeoutCount = to;
    }
  }
}
