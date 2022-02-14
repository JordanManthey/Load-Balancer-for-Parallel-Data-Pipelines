package com.pipelines;

public class MySQLManager extends DatabaseManager {

    public MySQLManager(String connectionURL, String username, String password, String excludeList) {
        super(connectionURL, username, password, excludeList);
        jdbcDriver = "com.mysql.jdbc.Driver";
    }

    @Override
    public String getVolumeQuery() {
        // Generating WHERE clause from user's exclude input
        String filter = "";
        for (String s : excludedSchemas) {
            filter += " and table_schema != '" + s + "'";
        }
        for (String t : excludedTables) {
            String currSchema = t.split("\\.")[0];
            String currTable = t.split("\\.")[1];
            filter += " and not (table_schema = '" + currSchema + "' and table_name = '" + currTable + "')";
        }

        String query = "SELECT table_schema,\n"
                + "     table_name,\n"
                + "     (data_length) / 1024 / 1024  as data_size\n"
                + "from information_schema.tables\n"
                + "where table_schema not in ('information_schema', 'mysql', 'performance_schema' ,'sys') and table_type = 'BASE TABLE'\n"
                + filter + "\n"
                + "order by data_size desc;";

        return query;
    }
}


