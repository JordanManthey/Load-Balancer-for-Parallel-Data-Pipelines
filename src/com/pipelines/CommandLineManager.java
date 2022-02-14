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
}
