package com.example;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.web.multipart.MultipartFile;
import com.opencsv.CSVReader;
import java.io.*;
import java.nio.file.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class FileProcessor {
    public void processAsyncFile(MultipartFile zipFile, String requestIdentifier) {
        String uploadDir = "uploads/";
        // Ensure uploads directory exists
        File uploadsDirFile = new File(uploadDir);
        if (!uploadsDirFile.exists()) {
            uploadsDirFile.mkdirs();
        }
        String unzipDir = uploadDir + "unzipped/" + requestIdentifier + "/";
        String dbUrl = "jdbc:h2:file:./data/upload-db";
        Map<String, String> csvTableMap = Map.of(
            "Safari55.csv", "stocks",
            "market_conditions.csv", "market_conditions",
            "customer_accounts_holdings.csv", "holdings",
            "customer_accounts.csv", "accounts"
        );
        Map<String, String> tableSchemas = Map.of(
            "stocks", "(Symbol VARCHAR, Security VARCHAR, GICS_Sector VARCHAR, GICS_Sub_Industry VARCHAR, CIK VARCHAR, Last_Close_Price DOUBLE)",
            "market_conditions", "(Type VARCHAR, Name VARCHAR, Condition VARCHAR)",
            "holdings", "(AccountID VARCHAR, Ticker VARCHAR, Qty INT, Price DOUBLE, PositionTotal DOUBLE)",
            "accounts", "(Account_ID VARCHAR, Age INT, Marital_Status VARCHAR, Dependents INT, Client_Industry VARCHAR, Residency_Zip VARCHAR, State VARCHAR, Account_Status VARCHAR, Annual_Income DOUBLE, Liquidity_Needs VARCHAR, Investment_Experience VARCHAR, Risk_Tolerance VARCHAR, Investment_Goals VARCHAR, Time_Horizon VARCHAR, Exclusions VARCHAR, SRI_Preferences VARCHAR, Tax_Status VARCHAR)"
        );
        Map<String, Integer> rowCounts = new HashMap<>();
        File uploadFile = null;
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

    // Helper: Unzip
    private void unzip(File zipFile, String destDir) throws IOException {
        byte[] buffer = new byte[1024];
        try (ZipInputStream zis = new ZipInputStream(new FileInputStream(zipFile))) {
            ZipEntry zipEntry = zis.getNextEntry();
            while (zipEntry != null) {
                File newFile = new File(destDir, zipEntry.getName());
                if (zipEntry.isDirectory()) {
                    newFile.mkdirs();
                } else {
                    new File(newFile.getParent()).mkdirs();
                    try (FileOutputStream fos = new FileOutputStream(newFile)) {
                        int len;
                        while ((len = zis.read(buffer)) > 0) {
                            fos.write(buffer, 0, len);
                        }
                    }
                }
                zipEntry = zis.getNextEntry();
            }
            zis.closeEntry();
        }
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
