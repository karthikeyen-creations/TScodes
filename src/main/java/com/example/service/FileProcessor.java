package com.example.service;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.web.multipart.MultipartFile;
import com.opencsv.CSVReader;
import java.io.*;
import java.nio.file.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.*;
import net.lingala.zip4j.ZipFile;

public class FileProcessor {
    public void processAsyncFile(MultipartFile zipFile, String requestIdentifier) {
        // String uploadDir = "uploads/";
        String uploadDir = Paths.get(System.getProperty("user.dir"), "uploads").toString();
        // Ensure uploads directory exists
        File uploadsDirFile = new File(uploadDir);
        if (!uploadsDirFile.exists()) {
            uploadsDirFile.mkdirs();
        }
        String unzipDir = uploadDir + "/unzipped" + requestIdentifier + "/";
        String dbUrl = "jdbc:h2:file:./data/upload-db";
        Map<String, String> csvTableMap = new HashMap<>();
        csvTableMap.put("Safari55.csv", "stocks");
        csvTableMap.put("market_conditions.csv", "market_conditions");
        csvTableMap.put("customer_accounts_holdings.csv", "holdings");
        csvTableMap.put("customer_accounts.csv", "accounts");

        Map<String, String> tableSchemas = new HashMap<>();
        tableSchemas.put("stocks", "(symbol VARCHAR PRIMARY KEY, security VARCHAR, gicssector VARCHAR, gicssubindustry VARCHAR, cik VARCHAR, lastcloseprice DOUBLE)");
        tableSchemas.put("market_conditions", "(type VARCHAR, name VARCHAR, condition VARCHAR, PRIMARY KEY (type, name))");
        tableSchemas.put("holdings", "(accountid VARCHAR, ticker VARCHAR, qty INT, price DOUBLE, positiontotal DOUBLE, PRIMARY KEY (accountid, ticker))");
        tableSchemas.put("accounts", "(accountid VARCHAR PRIMARY KEY, age INT, maritalstatus VARCHAR, dependents INT, clientindustry VARCHAR, residencyzip VARCHAR, state VARCHAR, accountstatus VARCHAR, annualincome DOUBLE, liquidityneeds VARCHAR, investmentexperience VARCHAR, risktolerance VARCHAR, investmentgoals VARCHAR, timehorizon VARCHAR, exclusions VARCHAR, sripreferences VARCHAR, taxstatus VARCHAR)");
        Map<String, Integer> rowCounts = new HashMap<>();
        File uploadFile = null;
        // Create tables in RebalReqs DB and close connection
        String rebalDbUrl = "jdbc:h2:file:./data/RebalReqs";
        try (Connection rebalConn = DriverManager.getConnection(rebalDbUrl, "sa", "")) {
            JdbcTemplate rebalJdbc = new JdbcTemplate(new SingleConnectionDataSource(rebalConn, true));
            // Drop tables if exist
            rebalJdbc.execute("DROP TABLE IF EXISTS reqs");
            rebalJdbc.execute("DROP TABLE IF EXISTS processes");
            // Create reqs table
            rebalJdbc.execute("CREATE TABLE reqs (reqid VARCHAR PRIMARY KEY, req VARCHAR, processflg1 BOOLEAN, processflg2 BOOLEAN)");
            // Create processes table
            rebalJdbc.execute("CREATE TABLE processes (process VARCHAR PRIMARY KEY, running BOOLEAN)");
            // Connection will be closed automatically at end of try-with-resources
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            // 1. Save zip to temp location
            uploadFile = new File(uploadDir, zipFile.getOriginalFilename());
            zipFile.transferTo(uploadFile);

            // 2. Unzip
            unzip(uploadFile, unzipDir);

            // 3. Process each CSV
            try (Connection conn = DriverManager.getConnection(dbUrl, "sa", "")) {
                JdbcTemplate jdbcTemplate = new JdbcTemplate(new SingleConnectionDataSource(conn, true));
                for (Map.Entry<String, String> entry : csvTableMap.entrySet()) {
                    String csvName = entry.getKey();
                    String tableName = entry.getValue();
                    String schema = tableSchemas.get(tableName);
                    File csvFile = new File(unzipDir, csvName);
                    if (!csvFile.exists()) continue;

                    // Drop table if exists
                    jdbcTemplate.execute("DROP TABLE IF EXISTS " + tableName);

                    // Create table
                    jdbcTemplate.execute("CREATE TABLE " + tableName + " " + schema);

                    // Parse and insert
                    int count = 0;
                    try (CSVReader reader = new CSVReader(new FileReader(csvFile))) {
                        String[] header = reader.readNext(); // skip header
                        // Convert header to lowercase and remove underscores for column mapping
                        for (int i = 0; i < header.length; i++) {
                            header[i] = header[i].replaceAll("_", "").toLowerCase();
                        }
                        String[] row;
                        while ((row = reader.readNext()) != null) {
                            String placeholders = String.join(",", Collections.nCopies(row.length, "?"));
                            jdbcTemplate.update("INSERT INTO " + tableName + " VALUES (" + placeholders + ")", (Object[]) row);
                            count++;
                        }
                    }
                    rowCounts.put(tableName, count);
                }
            }
            // 5. Log row counts
            rowCounts.forEach((table, count) -> 
                System.out.println("Inserted " + count + " rows into table " + table)
            );
        } catch (Exception e) {
            // 6. Log exceptions
            e.printStackTrace();
        } finally {
            // 7. Clean up temp files (optional)
            deleteDirectory(new File(unzipDir));
            if (uploadFile != null) uploadFile.delete();
        }
    }

    // Helper: Unzip using Zip4j
    private void unzip(File zipFile, String destDir) throws IOException {
        new ZipFile(zipFile).extractAll(destDir);
    }

    // Helper: Delete directory recursively
    private void deleteDirectory(File dir) {
        if (dir.isDirectory()) {
            File[] children = dir.listFiles();
            if (children != null) {
                for (File child : children) {
                    deleteDirectory(child);
                }
            }
        }
        dir.delete();
    }
}
