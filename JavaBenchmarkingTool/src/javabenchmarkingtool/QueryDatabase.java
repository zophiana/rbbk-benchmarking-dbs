package javabenchmarkingtool;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.stream.*;

public class QueryDatabase {

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

  // timeout constants: 300 s == 5 minutes
  private static final int TIMEOUT_SEC = 300;
  private static final long TIMEOUT_MS = TIMEOUT_SEC * 1000L;

  /**
   * For each DbConfig, loads its driver, opens a connection, then benchmarks
   * all queries in round-robin order N times.
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
    // 2) Benchmark each DB
    for (DbConfig cfg : configs) {
      appendLog(logFilePath,
              "INFO: ===== Benchmarking " + cfg.name + " =====");
      try (Connection conn = DriverManager.getConnection(
              cfg.url, cfg.user, cfg.pass)) {
        benchmarkDatabase(conn, cfg, queries, runs, logFilePath);
      } catch (SQLException e) {
        appendLog(logFilePath,
                "SEVERE: [" + cfg.name + "] Connection error: "
                + e.getMessage());
      }
    }
  }

  /**
   * Builds an interleaved sequence of queries (A,B,C,A,B,C…) and for each one
   * does a single execution, collecting stats.
   */
  private static void benchmarkDatabase(
          Connection conn,
          DbConfig cfg,
          List<String> queries,
          int runs,
          String logFilePath
  ) throws IOException {
    StatsCollector collector = new StatsCollector(queries, TIMEOUT_MS);

    // Generate round-robin sequence: A,B,C, A,B,C, … (runs times)
    List<String> sequence = IntStream.range(0, runs)
            .boxed()
            .flatMap(i -> queries.stream())
            .collect(Collectors.toList());

    // Execute each item in the sequence
    for (String sql : sequence) {
      try {
        ExecutionResult res = executeSingle(conn, sql);
        collector.record(sql, res);
        if (res.timeout) {
          appendLog(logFilePath,
                  String.format(
                          "WARNING: [%s] Timeout on query \"%s\"",
                          cfg.name, sql
                  ));
        }
      } catch (QueryExecutionException e) {
        appendLog(logFilePath,
                String.format(
                        "SEVERE: [%s] Error on query \"%s\": %s",
                        cfg.name,
                        sql,
                        e.getCause().getMessage()
                ));
      }
    }

    // Finally, log the stats for each query
    for (String sql : queries) {
      Stats stats = collector.getStats(sql);
      String rowsInfo = collector.getRows(sql)
              .map(Object::toString)
              .orElse("N/A");

      appendLog(
              logFilePath,
              "INFO: [" + cfg.name + "] SQL: \"" + sql + "\""
      );
      appendLog(
              logFilePath,
              "INFO: [" + cfg.name + "] Runs: " + runs
      );
      appendLog(
              logFilePath,
              "INFO: [" + cfg.name + "] Timeouts: "
              + stats.timeoutCount
      );
      appendLog(
              logFilePath,
              "INFO: [" + cfg.name + "] Rows returned: "
              + rowsInfo
      );
      appendLog(
              logFilePath,
              "INFO: [" + cfg.name + "] First run: "
              + stats.first + "ms"
      );
      appendLog(
              logFilePath,
              "INFO: [" + cfg.name + "] Last run: "
              + stats.last + "ms"
      );
      appendLog(
              logFilePath,
              "INFO: [" + cfg.name + "] Min time: "
              + stats.min + "ms"
      );
      appendLog(
              logFilePath,
              "INFO: [" + cfg.name + "] Max time: "
              + stats.max + "ms"
      );
      appendLog(
              logFilePath,
              String.format(
                      "INFO: [%s] Avg time: %.2fms",
                      cfg.name,
                      stats.average
              )
      );
      appendLog(
              logFilePath,
              String.format(
                      "INFO: [%s] Median: %.2fms",
                      cfg.name,
                      stats.median
              )
      );
      // blank line for readability
      appendLog(logFilePath, "");
    }
  }

  /**
   * Runs one SQL, applying the per-query timeout. Returns an ExecutionResult
   * indicating time or timeout.
   */
  private static ExecutionResult executeSingle(
          Connection conn,
          String query
  ) throws QueryExecutionException {
    try (PreparedStatement ps = conn.prepareStatement(query)) {
      ps.setQueryTimeout(TIMEOUT_SEC);
      long start = System.nanoTime();
      int rowCount = 0;

      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          rowCount++;
        }
        long took = (System.nanoTime() - start) / 1_000_000;
        return new ExecutionResult(took, rowCount, false);
      } catch (SQLException e) {
        // treat any SQLException here as a timeout
        return new ExecutionResult(TIMEOUT_MS, 0, true);
      }
    } catch (SQLException e) {
      throw new QueryExecutionException(e);
    }
  }

  // simple file-append
  private static void appendLog(String path, String line)
          throws IOException {
    try (FileWriter fw = new FileWriter(path, true)) {
      fw.write(line + "\n");
    }
  }

  /**
   * Encapsulates the result of one query run.
   */
  private static class ExecutionResult {

    final long time;
    final int rows;
    final boolean timeout;

    ExecutionResult(long time, int rows, boolean timeout) {
      this.time = time;
      this.rows = rows;
      this.timeout = timeout;
    }
  }

  /**
   * Thrown when preparing or executing the statement fails fatally.
   */
  private static class QueryExecutionException extends Exception {

    QueryExecutionException(Throwable cause) {
      super(cause);
    }
  }

  /**
   * Collects durations and first-seen row counts per SQL, then computes Stats
   * on demand.
   */
  private static class StatsCollector {

    private final long timeoutThreshold;
    private final Map<String, List<Long>> times = new HashMap<>();
    private final Map<String, Integer> rowsMap = new HashMap<>();

    StatsCollector(List<String> queries, long timeoutThreshold) {
      this.timeoutThreshold = timeoutThreshold;
      for (String q : queries) {
        times.put(q, new ArrayList<>());
      }
    }

    void record(String sql, ExecutionResult res) {
      times.get(sql).add(res.time);
      if (!res.timeout && !rowsMap.containsKey(sql)) {
        rowsMap.put(sql, res.rows);
      }
    }

    Stats getStats(String sql) {
      return new Stats(times.get(sql), timeoutThreshold);
    }

    Optional<Integer> getRows(String sql) {
      return Optional.ofNullable(rowsMap.get(sql));
    }
  }

  /**
   * Holds summary statistics over a list of durations.
   */
  private static class Stats {

    final long first, last, min, max;
    final double average, median, stdDev;
    final int timeoutCount;

    Stats(List<Long> values, long timeoutThreshold) {
      Collections.sort(values);
      int n = values.size();

      if (n > 0) {
        first = values.get(0);
        last = values.get(n - 1);
        min = values.get(0);
        max = values.get(n - 1);

        long sum = 0;
        for (long v : values) {
          sum += v;
        }
        average = (double) sum / n;

        if (n % 2 == 1) {
          median = values.get(n / 2);
        } else {
          median = (values.get(n / 2 - 1) + values.get(n / 2)) / 2.0;
        }

        double ss = 0;
        for (long v : values) {
          double d = v - average;
          ss += d * d;
        }
        stdDev = Math.sqrt(ss / n);
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
      for (long v : values) {
        if (v >= timeoutThreshold) {
          to++;
        }
      }
      timeoutCount = to;
    }
  }
}
