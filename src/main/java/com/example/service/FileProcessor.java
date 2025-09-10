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
        csvTableMap.put("market_conditions.csv", "market_conditions");
        csvTableMap.put("Safari55.csv", "stocks");
        csvTableMap.put("customer_accounts_holdings.csv", "holdings");
        csvTableMap.put("customer_accounts.csv", "accounts");

        Map<String, String> tableSchemas = new HashMap<>();
        tableSchemas.put("market_conditions", "(type VARCHAR, name VARCHAR, condition VARCHAR, PRIMARY KEY (type, name))");
        tableSchemas.put("stocks", "(symbol VARCHAR PRIMARY KEY, security VARCHAR, gicssector VARCHAR, gicssubindustry VARCHAR, cik VARCHAR, lastcloseprice DOUBLE, companysentiment VARCHAR, sectorsentiment VARCHAR, industrysentiment VARCHAR)");
        tableSchemas.put("holdings", "(accountid VARCHAR, ticker VARCHAR, qty INT, price DOUBLE, positiontotal DOUBLE, companysentiment VARCHAR, sectorsentiment VARCHAR, industrysentiment VARCHAR, sentimentweight INT, PRIMARY KEY (accountid, ticker))");
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
                            // For stocks table, add 3 empty columns for sentiment
                            if (tableName.equals("stocks")) {
                                String[] newRow = Arrays.copyOf(row, row.length + 3);
                                newRow[row.length] = ""; // companysentiment
                                newRow[row.length + 1] = ""; // sectorsentiment
                                newRow[row.length + 2] = ""; // industrysentiment
                                String placeholders = String.join(",", Collections.nCopies(newRow.length, "?"));
                                jdbcTemplate.update("INSERT INTO " + tableName + " VALUES (" + placeholders + ")", (Object[]) newRow);
                            } else if (tableName.equals("holdings")) {
                                String[] newRow = Arrays.copyOf(row, row.length + 4);
                                newRow[row.length] = ""; // companysentiment
                                newRow[row.length + 1] = ""; // sectorsentiment
                                newRow[row.length + 2] = ""; // industrysentiment
                                newRow[row.length + 3] = ""; // sentimentweight
                                String placeholders = String.join(",", Collections.nCopies(newRow.length, "?"));
                                jdbcTemplate.update("INSERT INTO " + tableName + " VALUES (" + placeholders + ")", (Object[]) newRow);
                            } else {
                                String placeholders = String.join(",", Collections.nCopies(row.length, "?"));
                                jdbcTemplate.update("INSERT INTO " + tableName + " VALUES (" + placeholders + ")", (Object[]) row);
                            }
                            count++;
                        }
                    }
                    rowCounts.put(tableName, count);
                }

                // After all tables are loaded, update stocks sentiment columns from market_conditions
                // companysentiment
                jdbcTemplate.update(
                    "UPDATE stocks SET companysentiment = (SELECT condition FROM market_conditions WHERE type = 'Security' AND name = stocks.symbol) WHERE symbol IN (SELECT name FROM market_conditions WHERE type = 'Security')"
                );
                // sectorsentiment
                jdbcTemplate.update(
                    "UPDATE stocks SET sectorsentiment = (SELECT condition FROM market_conditions WHERE type = 'Sector' AND name = stocks.gicssector) WHERE gicssector IN (SELECT name FROM market_conditions WHERE type = 'Sector')"
                );
                // industrysentiment
                jdbcTemplate.update(
                    "UPDATE stocks SET industrysentiment = (SELECT condition FROM market_conditions WHERE type = 'Industry' AND name = stocks.gicssubindustry) WHERE gicssubindustry IN (SELECT name FROM market_conditions WHERE type = 'Industry')"
                );

                // Now update holdings sentiment columns from stocks table
                // companysentiment
                jdbcTemplate.update(
                    "UPDATE holdings SET companysentiment = (SELECT companysentiment FROM stocks WHERE symbol = holdings.ticker) WHERE ticker IN (SELECT symbol FROM stocks)"
                );
                // sectorsentiment
                jdbcTemplate.update(
                    "UPDATE holdings SET sectorsentiment = (SELECT sectorsentiment FROM stocks WHERE symbol = holdings.ticker) WHERE ticker IN (SELECT symbol FROM stocks)"
                );
                // industrysentiment
                jdbcTemplate.update(
                    "UPDATE holdings SET industrysentiment = (SELECT industrysentiment FROM stocks WHERE symbol = holdings.ticker) WHERE ticker IN (SELECT symbol FROM stocks)"
                );

                // Calculate sentimentweight for each row in holdings
                jdbcTemplate.update(
                    "UPDATE holdings SET sentimentweight = " +
                    "(CASE companysentiment WHEN 'Negative' THEN -1 WHEN 'Neutral' THEN 0 WHEN 'Positive' THEN 1 ELSE 0 END) + " +
                    "(CASE sectorsentiment WHEN 'Negative' THEN -1 WHEN 'Neutral' THEN 0 WHEN 'Positive' THEN 1 ELSE 0 END) + " +
                    "(CASE industrysentiment WHEN 'Negative' THEN -1 WHEN 'Neutral' THEN 0 WHEN 'Positive' THEN 1 ELSE 0 END)"
                );
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
