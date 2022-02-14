package com.pipelines;

import java.util.Scanner;

public class CommandLineManager {


    public static String NUM_APPS;
    public static String NUM_CORES;
    public static String EXCLUDE;
    public static String SOURCE_TYPE;

    public CommandLineManager() {

    }

    // Get user input and set static variables
    public static void getUserInput() {

        Scanner scan = new Scanner(System.in);
        System.out.println("Enter source JDBC URL: ");
        String connectionURL = scan.nextLine();
        System.out.println("Enter schemas and tables to exclude:");
        EXCLUDE = scan.nextLine().replaceAll("\\s+","");
        System.out.println("Enter username: ");
        String username = scan.nextLine();
        System.out.println("Enter password: ");
        String password = scan.nextLine();
        System.out.println("Enter # IL applications: ");
        NUM_APPS = scan.nextLine();
        System.out.println("Enter # Striim cores: ");
        NUM_CORES = scan.nextLine();
        scan.close();
    }

    public static void getTQLInput() {
        Scanner scan = new Scanner(System.in);
        System.out.println("Enter target JDBC URL: ");
        String url = scan.nextLine();
        System.out.println("Enter username: ");
        String user = scan.nextLine();
        System.out.println("Enter password: ");
        String pass = scan.nextLine();
        System.out.println("Enter application name: ");
        String appName = scan.nextLine();
        System.out.println("Enter source name: ");
        String sourceName = scan.nextLine();
        System.out.println("Enter stream name: ");
        String streamName = scan.nextLine();
        System.out.println("Enter target name: ");
        String targetName = scan.nextLine();
        System.out.println("");
        scan.close();
    }

    public static boolean proposeTableSplit(String tableName, double tableVolume, double goalDifference) {
        System.out.println("WARN: Table '" + tableName + "' exceeds the app goal by " + goalDifference + " MB (" + tableVolume + " MB).");
        System.out.println("Split this table by ROWID? (Y/N): ");
        Scanner scan = new Scanner(System.in);
        return scan.nextLine().equals("Y");
    }
}
