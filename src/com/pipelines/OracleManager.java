package com.pipelines;

import java.sql.*;
import java.util.ArrayList;
import java.util.Map;

public class OracleManager extends DatabaseManager {

    private Map<String, ArrayList<String>> partitionMap;

    public OracleManager(String connectionURL, String username, String password, String excludeList) {
        super(connectionURL, username, password, excludeList);
        this.jdbcDriver = "oracle.jdbc.driver.OracleDriver";
        this.canPartitionTables = true;
    }

    @Override
    public String getVolumeQuery() {
        // Generating WHERE clause from user's exclude input
        String filter = "";
        for (String s : excludedSchemas) {
            filter += " AND OWNER != '" + s + "'";
        }
        for (String t : excludedTables) {
            String currSchema = t.split("\\.")[0];
            String currTable = t.split("\\.")[1];
            filter += " AND NOT (OWNER = '" + currSchema + "' AND SEGMENT_NAME = '" + currTable + "')";
        }

        String query = "SELECT OWNER as \"SCHEMA\", SEGMENT_NAME as \"TABLE\", BYTES/1024/1024 as \"DATA_SIZE_MB\"\n"
                + "FROM dba_segments \n"
                + "WHERE SEGMENT_TYPE = 'TABLE'" + filter + "\n"
                + "ORDER BY BYTES DESC";

        return query;
    }

    // Generates queries to be added to DBReader "Query" property. These queries split Oracle tables into partitions via ROWID. The goal here is to remove bottlenecks in Striim apps.
    @Override
    public void partitionTable(String tableName, int numPartitions) throws ClassNotFoundException, SQLException {

        // This query generates the Striim DBReader query string needed for each partition of the bottleneck table.
        String query = "select 'select * from " + tableName + " where rowid between \\''' || dbms_rowid.rowid_create( 1, data_object_id, lo_fno, lo_block, 0 ) || '\\'' and \\''' || dbms_rowid.rowid_create( 1, data_object_id, hi_fno, hi_block, 10000 ) || '\\''' as QUERY\n"
                + "from (\n"
                + "select distinct grp,\n"
                + "first_value(relative_fno)\n"
                + "over (partition by grp order by relative_fno, block_id\n"
                + "rows between unbounded preceding and unbounded following) lo_fno,\n"
                + "first_value(block_id )\n"
                + "over (partition by grp order by relative_fno, block_id\n"
                + "rows between unbounded preceding and unbounded following) lo_block,\n"
                + "last_value(relative_fno)\n"
                + "over (partition by grp order by relative_fno, block_id\n"
                + "rows between unbounded preceding and unbounded following) hi_fno,\n"
                + "last_value(block_id+blocks-1)\n"
                + "over (partition by grp order by relative_fno, block_id\n"
                + "rows between unbounded preceding and unbounded following) hi_block,\n"
                + "sum(blocks) over (partition by grp) sum_blocks\n"
                + "from (\n"
                + "select relative_fno,\n"
                + "block_id,\n"
                + "blocks,\n"
                + "trunc( (sum(blocks) over (order by relative_fno, block_id)-0.01) /\n"
                + "(sum(blocks) over ()/" + numPartitions +") ) grp\n"
                + "from dba_extents\n"
                + "where owner || '.' || segment_name = upper('" + tableName + "')\n"
                + "order by block_id\n"
                + ")\n"
                + "),\n"
                + "(select data_object_id from dba_objects where owner ||'.'|| object_name = upper('" + tableName + "') )";


        Class.forName(jdbcDriver);
        Connection conn = DriverManager.getConnection(connectionURL, username, password);
        Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        conn.setAutoCommit(true);
        ResultSet rs = stmt.executeQuery(query);

        while (rs.next()) {

            ArrayList<String> newValue;
            if (partitionMap.containsKey(tableName)) {
                newValue = partitionMap.get(tableName);
            } else {
                newValue = new ArrayList<>();
            }
            newValue.add(rs.getString(1));
            partitionMap.put(tableName, newValue);
        }
        conn.close();
    }
}


