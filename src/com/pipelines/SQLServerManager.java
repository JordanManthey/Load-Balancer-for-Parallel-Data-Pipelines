package com.pipelines;

public class SQLServerManager extends DatabaseManager {

    public SQLServerManager(String connectionURL, String username, String password, String excludeList) {
        super(connectionURL, username, password, excludeList);
        jdbcDriver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    }

    @Override
    public String getVolumeQuery() {
        // Generating WHERE clause from user's exclude input
        String filter = "where schema_name(tab.schema_id) != ";
        boolean firstSchema = true;
        for (String s : excludedSchemas) {
            if (firstSchema) {
                firstSchema = false;
                filter += "'" + s + "'";
            } else {
                filter += " and schema_name(tab.schema_id) != '" + s + "'";
            }
        }
        for (String t : excludedTables) {
            String currSchema = t.split("\\.")[0];
            String currTable = t.split("\\.")[1];
            filter += " and not (schema_name(tab.schema_id) = '" + currSchema + "' and tab.name = '" + currTable + "')";
        }

        String query = "select schema_name(tab.schema_id) as [schema], \n"
                + "    +tab.name as [table],\n"
                + "    cast(sum(spc.used_pages * 8)/1024.00 as numeric(36, 2)) as used_mb\n"
                + "from sys.tables tab\n"
                + "join sys.indexes ind \n"
                + "     on tab.object_id = ind.object_id\n"
                + "join sys.partitions part \n"
                + "     on ind.object_id = part.object_id and ind.index_id = part.index_id\n"
                + "join sys.allocation_units spc\n"
                + "     on part.partition_id = spc.container_id\n"
                + filter + "\n"
                + "group by schema_name(tab.schema_id), tab.name\n"
                + "order by sum(spc.used_pages) desc;";

        return query;
    }
}


