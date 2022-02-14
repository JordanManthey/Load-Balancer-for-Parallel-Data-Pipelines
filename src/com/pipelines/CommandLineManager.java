package com.pipelines;

import java.util.Scanner;

public class CommandLineManager {

    // Returns an instance of the correct DBManager from user input.
    public static DatabaseManager buildDBManagerFromUserInput() {

        Scanner scan = new Scanner(System.in);
        System.out.println("Enter source JDBC URL: ");
        String connectionURL = scan.nextLine();
        System.out.println("Enter schemas and tables to exclude:");
        String excludeList = scan.nextLine().replaceAll("\\s+","");
        System.out.println("Enter username: ");
        String username = scan.nextLine();
        System.out.println("Enter password: ");
        String password = scan.nextLine();
        System.out.println("Enter # IL applications: ");
        int numApps = Integer.valueOf(scan.nextLine());
        System.out.println("Enter # Striim cores: ");
        int numCores = Integer.valueOf(scan.nextLine());
        scan.close();

        String sourceType = connectionURL.split(":", 3)[1];

        switch (sourceType) {
            case "postgresql":
                return new PostgresManager(connectionURL, username, password, excludeList);
            case "sqlserver":
                return new SQLServerManager(connectionURL, username, password, excludeList);
            case "oracle":
                return new OracleManager(connectionURL, username, password, excludeList);
            case "mysql":
                return new MySQLManager(connectionURL, username, password, excludeList);
            default:
                return null;
        }
    }

    public static FileGenerator buildFileGeneratorFromUserInput() {
        Scanner scan = new Scanner(System.in);
        System.out.println("Enter target JDBC URL: ");
        String sourceURL = scan.nextLine();
        System.out.println("Enter username: ");
        String sourceUser = scan.nextLine();
        System.out.println("Enter password: ");
        String sourcePass = scan.nextLine();
        System.out.println("Enter target JDBC URL: ");
        String targetURL = scan.nextLine();
        System.out.println("Enter username: ");
        String targetUser = scan.nextLine();
        System.out.println("Enter password: ");
        String targetPass = scan.nextLine();
        System.out.println("");
        scan.close();

        return new FileGenerator(sourceURL, sourceUser, sourcePass, targetURL, targetUser, targetPass);
    }

    public static boolean proposeTableSplit(String tableName, double tableVolume, double goalDifference) {
        System.out.println("WARN: Table '" + tableName + "' exceeds the app goal by " + goalDifference + " MB (" + tableVolume + " MB).");
        System.out.println("Split this table by ROWID? (Y/N): ");
        Scanner scan = new Scanner(System.in);
        return scan.nextLine().equals("Y");
    }
}
