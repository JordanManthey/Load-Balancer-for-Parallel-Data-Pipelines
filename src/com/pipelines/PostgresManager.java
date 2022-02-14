package com.pipelines;

public class PostgresManager extends DatabaseManager {

    public PostgresManager(String connectionURL, String username, String password, String excludeList) {
        super(connectionURL, username, password, excludeList);
        this.jdbcDriver = "org.postgresql.Driver";
    }

    @Override
    public String getVolumeQuery() {
        // Generating WHERE clause from user's exclude input.
        String filter = "where schemaname != ";
        boolean firstSchema = true;
        for (String s : excludedSchemas) {
            if (firstSchema) {
                firstSchema = false;
                filter += "'" + s + "'";
            } else {
                filter += " and schemaname != '" + s + "'";
            }
        }
        for (String t : excludedTables) {
            String currSchema = t.split("\\.")[0];
            String currTable = t.split("\\.")[1];
            filter += " and not (schemaname = '" + currSchema + "' and relname = '" + currTable + "')";
        }

        String query = "select schemaname as table_schema,\n"
                + "    relname as table_name,\n"
                + "    pg_relation_size(relid) / 1000000.0 as data_size_mb\n"
                + "from pg_catalog.pg_statio_user_tables\n"
                + filter + "\n"
                + "order by pg_relation_size(relid) desc;";

        return query;
    }
}


