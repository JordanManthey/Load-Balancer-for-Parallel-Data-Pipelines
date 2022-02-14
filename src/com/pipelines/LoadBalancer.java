package com.pipelines;

import javax.sql.rowset.CachedRowSet;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class LoadBalancer {

    public int numApps;
    public int numCores;

    // Allocates parallel threads to each Striim application proportional to the data volume its responsible for
    public void allocateCores(Map<Integer, String[]> map, double sum) {

        for (Integer key : map.keySet()) {

            if (!map.get(key)[1].equals("")) {
                int cores = (int) ((numCores * Double.valueOf(map.get(key)[1])) / sum);
                String[] newVal = map.get(key);
                newVal[2] = String.valueOf(cores);
                map.put(key, newVal);
            }
        }
    }

    // Removes Striim apps that are not needed to maintain data volume goals. This can happen if a table creates a bottleneck that makes the original data volume goals obsolete.
    public void removeUnneededApps(Map<Integer, String[]> appsToTables) {

        ArrayList<Integer> keysToRemove = new ArrayList<Integer>();

        for (Integer key : appsToTables.keySet()) {

            System.out.print("App" + key + ": ");
            if (appsToTables.get(key)[1].equals("")) {
                System.out.println(appsToTables.get(key)[1] + "0 MB, 0 tables, 0 threads (App not needed)");
                keysToRemove.add(key);
            } else {
                int tableCount = appsToTables.get(key)[0].split(";").length;
                System.out.println(appsToTables.get(key)[1] + " MB, " + tableCount + " tables, " + appsToTables.get(key)[2] + " threads");
            }
        }

        // Must remove keys outside of first loop to not break java map concurrency.
        for (Integer k : keysToRemove) {
            appsToTables.remove(k);
        }
        System.out.println("");
    }

    // Returns the total data volume and the goal data volume for each Striim application
    public double[] getGoalAppSize(ResultSet rs) throws SQLException {

        double sum = 0;

        while (rs.next())
        {
            sum += Double.valueOf(rs.getString(3));
        }

        return new double[] {sum, sum / Double.valueOf(numApps)};
    }

    // Finds the optimal distribution of tables for each Striim application based on data volumes. Returns a mapping of { App # : tables, data volume }.
    public Map<Integer, String[]> getAppMap(CachedRowSet rs, double sum, double goal, DatabaseManager dbManager) throws SQLException, ClassNotFoundException {

        int tableCount = 0;
        double tolerance = 0.05;
        Map<Integer, String[]> appsToTables = new HashMap<Integer, String[]>();

        System.out.println("Total data volume: " + sum + " MB");
        System.out.println("Data volume goal per app: " + goal + " MB");

        // For each app, loops through the available tables:volumes until the data volume goal for that given app has been met (with an acceptance tolerance of +- 5%)
        for (int i = 1; i <= Integer.valueOf(numApps); i++) {

            double currTotal = 0;

            appsToTables.put(Integer.valueOf(i), new String[]{"","", ""});

            rs.beforeFirst();
            while (rs.next()) {
                double tableVol = Double.valueOf(rs.getString(3));

                // Put all remaining tables in the last Striim application
                if (i == Integer.valueOf(numApps)) {
                    currTotal += tableVol;
                    appsToTables.put(i, new String[] {appsToTables.get(i)[0] + rs.getString(1) + "." + rs.getString(2) + ";", String.valueOf(currTotal), ""});
                    tableCount++;
                    continue;
                }

                // This table is a bottleneck if its volume alone exceeds the app data volume goal. Either split table or place in own application.
                if (tableVol > goal) {
                    currTotal += tableVol;
                    double diff = tableVol - goal;

                    if (dbManager.canPartitionTables) {
                        // Split this table by ROWID
                        String tableName = rs.getString(1) + "." + rs.getString(2);
                        if (CommandLineManager.proposeTableSplit(tableName, tableVol, diff)) {
                            // Calculate # chunks based on the size of our app data volume goal
                            int numPartitions = 0;
                            double tempVol = tableVol;
                            while (tempVol > 0) {
                                tempVol -= goal;
                                numPartitions++;
                            }

                            // Partitions this table and populates this dbManager's partitionMap
                            dbManager.partitionTable(rs.getString(1) + "." + rs.getString(2), numPartitions);

                            for (int n = 0; n < numPartitions; n++) {
                                if (n == numPartitions - 1) {
                                    double remainderVol = tableVol - ((tableVol/numPartitions)*(numPartitions-1));
                                    appsToTables.put(i, new String[] {rs.getString(1) + "." + rs.getString(2) + ";", String.valueOf(remainderVol), ""});
                                } else {
                                    appsToTables.put(i, new String[] {rs.getString(1) + "." + rs.getString(2) + ";", String.valueOf(tableVol/numPartitions), ""});
                                    i++;
                                }
                            }

                            // Calculate new volume goal for remaining apps now that we split a table which forces a distribution change
                            int appsRemaining = numApps - numPartitions;
                            goal = (sum - tableVol) / appsRemaining;
                            sum -= tableVol;
                            System.out.println("Updating new goal for the remaining apps (due to table split): " + goal + " MB");

                            rs.deleteRow();
                            break;

                        // Give table its own application
                        } else {
                            //scan.close();
                            appsToTables.put(i, new String[] {appsToTables.get(i)[0] + rs.getString(1) + "." + rs.getString(2) + ";", String.valueOf(currTotal), ""});
                            tableCount++;
                            rs.deleteRow();

                            goal = tableVol-(tableVol*tolerance);
                            System.out.println("Updating new goal per app (due to table bottleneck): " + goal + " MB");
                            break;
                        }
                    // Give table its own application
                    } else {
                        System.out.println("WARN: Table '" + rs.getString(1) + "." + rs.getString(2) + "' exceeds the app goal by " + diff + " MB. Consider splitting this table.");
                        appsToTables.put(i, new String[] {appsToTables.get(i)[0] + rs.getString(1) + "." + rs.getString(2) + ";", String.valueOf(currTotal), ""});
                        tableCount++;
                        rs.deleteRow();

                        goal = tableVol-(tableVol*tolerance);
                        System.out.println("Updating new goal per app: " + goal + " MB");
                        break;
                    }
                // Add this table to current application if it's still under our goal and continue searching for this app
                } else if (tableVol + currTotal < goal-(goal*tolerance)) {
                    currTotal += tableVol;
                    appsToTables.put(i, new String[] {appsToTables.get(i)[0] + rs.getString(1) + "." + rs.getString(2) + ";", String.valueOf(currTotal), ""});
                    tableCount++;
                    rs.deleteRow();

                // Add this table if it meets our app goal and move onto next app
                } else if (tableVol + currTotal < goal+(goal*tolerance) && tableVol + currTotal > goal-(goal*tolerance)) {
                    currTotal += tableVol;
                    appsToTables.put(i, new String[] {appsToTables.get(i)[0] + rs.getString(1) + "." + rs.getString(2) + ";", String.valueOf(currTotal), ""});
                    tableCount++;
                    rs.deleteRow();
                    break;

                // Skip this table if it will send us over our app goal
                } else if (tableVol + currTotal > goal+(goal*tolerance)) {
                    continue;
                }
            }

            // This means a table partition forced you to use up the NUM_APPS before it could distribute workload
            if (i == Integer.valueOf(numApps) && currTotal > goal+(goal*tolerance*2)) {
                System.out.println("WARN: Splitting tables has forced App" + i + " to exceed the app goal. Consider provisioning more apps or splitting less tables.");
            }
        }

        System.out.println();
        System.out.println("Processed " + tableCount + " tables:");
        System.out.println();
        return appsToTables;
    }

}
