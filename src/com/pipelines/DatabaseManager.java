package com.pipelines;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetProvider;

public abstract class DatabaseManager {

    protected static String jdbcDriver;
    protected String sourceType;
    protected String connectionURL;
    protected String username;
    protected String password;
    protected ArrayList<String> excludedTables;
    protected ArrayList<String> excludedSchemas;

    public DatabaseManager(String connectionURL, String username, String password, String excludedString) {

        this.connectionURL = connectionURL;
        this.username = username;
        this.password = password;
        this.sourceType = connectionURL.split(":", 3)[1];

        // Separate exclude schemas and tables
        for (String e : excludedString.split(";")) {
            if (e.contains(".")) {
                excludedTables.add(e);
            } else {
                excludedSchemas.add(e);
            }
        }
    }

    // Returns the table volume query for this database.
    public abstract String getVolumeQuery();

    // Queries the source database for data volume information for each table and returns the results in desc order.
    public CachedRowSet getTableDataVolumes() throws IOException, ClassNotFoundException, SQLException {

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
}
