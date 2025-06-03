package javabenchmarkingtool;

import org.junit.Test;

public class QueryDatabaseTest {
    
    public QueryDatabaseTest() {}
    
    @Test
    public void testExecuteQueryGetAll() throws Exception {
        String query = "SELECT * FROM CRASH_DATA";
        String driver = "org.hsqldb.jdbc.JDBCDriver";
        String url = "jdbc:hsqldb:hsql://localhost/";
        String u = "SA";
        String p = "";
        
        QueryDatabase instance = new QueryDatabase();
        instance.LOG_FILE_PATH = "/home/info/Dokumente/getAll.log";
        instance.executeQuery(query, driver, url, u, p);
    }
    
    @Test
    public void testExecuteQueryOrderByVehicleOne() throws Exception {
        String query = "SELECT * FROM CRASH_DATA ORDER BY contributing_factor_1";
        String driver = "org.hsqldb.jdbc.JDBCDriver";
        String url = "jdbc:hsqldb:hsql://localhost/";
        String u = "SA";
        String p = "";
        QueryDatabase instance = new QueryDatabase();
        instance.LOG_FILE_PATH = "/home/info/Dokumente/orderBy.log";
        instance.executeQuery(query, driver, url, u, p);
    }
    
    @Test
    public void testExecuteQueryWhereNumDeathsLargerFive() throws Exception {
        String query = "SELECT * FROM CRASH_DATA WHERE persons_killed > 5";
        String driver = "org.hsqldb.jdbc.JDBCDriver";
        String url = "jdbc:hsqldb:hsql://localhost/";
        String u = "SA";
        String p = "";
        QueryDatabase instance = new QueryDatabase();
        instance.LOG_FILE_PATH = "/home/info/Dokumente/deathLargerFive.log";
        instance.executeQuery(query, driver, url, u, p);
    }
    
}
