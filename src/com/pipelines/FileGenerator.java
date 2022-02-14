package com.pipelines;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Map;

public class FileGenerator {

    private String sourceURL;
    private String sourceUser;
    private String sourcePass;
    private String targetURL;
    private String targetUser;
    private String targetPass;

    public FileGenerator(String sourceURL, String sourceUser, String sourcePass, String targetURL, String targetUser, String targetPass) {
        this.sourceURL = sourceURL;
        this.sourceUser = sourceUser;
        this.sourcePass = sourcePass;
        this.targetURL = targetURL;
        this.targetUser = targetUser;
        this.targetPass = targetPass;
    }

    // Generates a TQL file for each application in the same directory as this JAR file.
    public void generateTQL(Map<Integer, String[]> appMap, Map<String, ArrayList<String>> partitionMap) throws IOException {

        for (Integer key : appMap.keySet()) {

            String sourceTables = appMap.get(key)[0];
            String partitionTable = sourceTables.substring(0, sourceTables.length()-1); // remove trailing ";"
            String query = "";
            String numThreads = appMap.get(key)[2];

            // 0 is invalid input for Parallel Threads property
            if (numThreads.equals("0")) {
                numThreads = "";
            }

            // Sets the DBReader Query property if this app is responsible for a splitted Oracle table
            if (partitionMap.containsKey(partitionTable)) {
                ArrayList<String> queryList = partitionMap.get(partitionTable);
                query = queryList.remove(0);
                partitionMap.put(partitionTable, queryList);
            }

            ArrayList<String> schemas = DatabaseManager.getSchemas(appMap, key);
            String targetTables = "";
            for (String s : schemas) {
                targetTables += s + ".%," + s + ".%;";
            }

            int appNum = 0;
            // Formating TQL file text
            ArrayList<String> lines = new ArrayList<String>();

            lines.add("CREATE APPLICATION app" + appNum + "_" + key + ";");
            lines.add("");
            lines.add("CREATE OR REPLACE SOURCE source" + appNum + "_" + key + " USING Global.DatabaseReader (");
            lines.add("  DatabaseProviderType: 'Default',");
            lines.add("  Password: '" + sourcePass + "',");
            lines.add("  adapterName: 'DatabaseReader',");
            lines.add("  QuiesceOnILCompletion: true,");
            lines.add("  FetchSize: 10000,");

            if (query != "") {
                lines.add("  Query: '" + query + "',");
            }

            lines.add("  Tables: '" + sourceTables + "',");
            lines.add("  ConnectionURL: '" + sourceURL + "',");
            lines.add("  Username: '" + sourceUser + "' )");
            lines.add("OUTPUT TO stream" + appNum + "_" + key + ";");
            lines.add("");
            lines.add("CREATE OR REPLACE TARGET target" + appNum + "_" + key + " USING Global.DatabaseWriter (");
            lines.add("  ParallelThreads: '" + numThreads + "',");
            lines.add("  ConnectionRetryPolicy: 'retryInterval=30, maxRetries=3',");
            lines.add("  BatchPolicy: 'EventCount:10000,Interval:60',");
            lines.add("  CommitPolicy: 'EventCount:10000,Interval:60',");
            lines.add("  CheckPointTable: 'CHKPOINT',");
            lines.add("  StatementCacheSize: '50',");
            lines.add("  Tables: '" + targetTables + "',");
            lines.add("  DatabaseProviderType: 'Default',");
            lines.add("  Password: '" + targetPass + "',");
            lines.add("  ConnectionURL: '" + targetURL + "',");
            lines.add("  PreserveSourceTransactionBoundary: 'false',");
            lines.add("  Username: '" + targetUser + "',");
            lines.add("  adapterName: 'DatabaseWriter' )");
            lines.add("INPUT FROM stream" + appNum + "_" + key + ";");
            lines.add("");
            lines.add("END APPLICATION app" + appNum + "_" + key + ";");
            lines.add("");

            Path file = Paths.get("app" + appNum + "_" + key + ".tql");
            Files.write(file, lines, StandardCharsets.UTF_8);
        }
        System.out.println("TQLs generated successfully.");
    }
}
