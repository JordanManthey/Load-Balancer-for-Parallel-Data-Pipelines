package com.pipelines;

import javax.sql.rowset.CachedRowSet;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;

public class Main {

    public static void main(String[] args) throws SQLException, ClassNotFoundException, IOException {

        DatabaseManager dbManager = CommandLineManager.buildDBManagerFromUserInput();
        CachedRowSet tableVolumesDesc = dbManager.getTableDataVolumes();
        LoadBalancer loadBalancer = new LoadBalancer();
        double[] volumeSumAndGoal = loadBalancer.getGoalAppSize(tableVolumesDesc);
        double volumeSumMB = volumeSumAndGoal[0];
        double volumeGoalPerAppMB = volumeSumAndGoal[1];
        Map<Integer, String[]> applicationToTablesMap = loadBalancer.generateAppToTablesMap(tableVolumesDesc, volumeSumMB, volumeGoalPerAppMB, dbManager);
        loadBalancer.allocateCores(applicationToTablesMap, volumeSumMB);
        loadBalancer.removeExcessApps(applicationToTablesMap);
        FileGenerator fileGenerator = CommandLineManager.buildFileGeneratorFromUserInput();
        fileGenerator.generateTQL(applicationToTablesMap, dbManager.partitionMap);
    }
}
