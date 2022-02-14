package com.pipelines;

import java.sql.*;
import java.util.ArrayList;
import java.util.Map;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetProvider;

public abstract class DatabaseManager {

    protected static String jdbcDriver;
    protected String connectionURL;
    protected String username;
    protected String password;
    protected ArrayList<String> excludedTables;
    protected ArrayList<String> excludedSchemas;
    protected boolean canPartitionTables;
    protected Map<String, ArrayList<String>> partitionMap;

    public DatabaseManager(String connectionURL, String username, String password, String excludedString) {

        this.connectionURL = connectionURL;
        this.username = username;
        this.password = password;
        this.canPartitionTables = false;

        // Separate exclude schemas and tables
        for (String e : excludedString.split(";")) {
            if (e.contains(".")) {
                excludedTables.add(e);
            } else {
                excludedSchemas.add(e);
            }
        }
    }

    // Returns a list of all schemas
    public static ArrayList<String> getSchemas(Map<Integer, String[]> map, Integer currKey) {

        ArrayList<String> schemas = new ArrayList<String>();
        String[] schemaTables = map.get(currKey)[0].split(";");
        for (String s : schemaTables) {
            int iend = s.indexOf(".");
            s = s.substring(0, iend);
            if (!schemas.contains(s)) {
                schemas.add(s);
            }
        }
        return schemas;
    }

    // Returns the table volume query for this database.
    public abstract String getVolumeQuery();

    // Queries the source database for data volume information for each table and returns the results in desc order.
    public CachedRowSet getTableDataVolumes() throws ClassNotFoundException, SQLException {

        String query = getVolumeQuery();
        // Connect to source database and execute query
        Class.forName(jdbcDriver);
        System.out.println("Connecting to the database...");
        System.out.println();

        Connection conn = DriverManager.getConnection(connectionURL, username, password);
        Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        conn.setAutoCommit(true);
        ResultSet rs = stmt.executeQuery(query);

        // Cache the resultset so we can close the connection and continue to work with the queried data
        rs.beforeFirst();
        CachedRowSet crs = RowSetProvider.newFactory().createCachedRowSet();
        crs.populate(rs);
        conn.close();
        return crs;
    }

    public void partitionTable(String tableName, int numPartitions) throws ClassNotFoundException, SQLException {
        return;
    }
}
