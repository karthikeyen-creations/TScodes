
package com.example;

import java.sql.*;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import com.google.gson.Gson;

public class RebalanceHandler {
    public String handleAsyncRebalRequest(RebalanceRequest rebalRequest) {
        // Write rebalRequest to RebalReqs DB
        String rebalDbUrl = "jdbc:h2:file:./data/RebalReqs";
        String reqid = rebalRequest.getRequestIdentifier();
        String req = new Gson().toJson(rebalRequest.getAccountRebalanceCriterias());
        try (Connection conn = DriverManager.getConnection(rebalDbUrl, "sa", "")) {
            JdbcTemplate jdbc = new JdbcTemplate(new SingleConnectionDataSource(conn, true));
            jdbc.update("INSERT INTO reqs (reqid, req, processflg1, processflg2) VALUES (?, ?, ?, ?)", reqid, req, false, false);
        } catch (Exception e) {
            e.printStackTrace();
        }
        runRebalanceAsync();
        return "done";
    }

    // True async function using a new thread
    public void runRebalanceAsync() {
        System.out.println("runRebalanceAsync: starting new thread");
        new Thread(() -> {
            String flag1 = "Y";
            while ("Y".equals(flag1)) {
                processRebalanceRequests();
                // Check if any unprocessed requests remain
                String rebalDbUrl = "jdbc:h2:file:./data/RebalReqs";
                boolean hasUnprocessed = false;
                try (Connection conn = DriverManager.getConnection(rebalDbUrl, "sa", "")) {
                    try (PreparedStatement ps = conn.prepareStatement("SELECT reqid FROM reqs WHERE processflg1 = false LIMIT 1")) {
                        try (ResultSet rs = ps.executeQuery()) {
                            if (rs.next()) {
                                hasUnprocessed = true;
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                if (!hasUnprocessed) {
                    flag1 = "N";
                }
            }
        }).start();
    }

    // Method containing the thread's logic
    private void processRebalanceRequests() {
        System.out.println("runRebalanceAsync: inside thread, reading RebalReqs table");
        String rebalDbUrl = "jdbc:h2:file:./data/RebalReqs";
        try (Connection conn = DriverManager.getConnection(rebalDbUrl, "sa", "")) {
            try (PreparedStatement ps = conn.prepareStatement("SELECT reqid, req FROM reqs WHERE processflg1 = false LIMIT 1")) {
                System.out.println("runRebalanceAsync: checking for unprocessed requests");
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String reqid = rs.getString("reqid");
                        String req = rs.getString("req");
                        System.out.println("Processing reqid: " + reqid + ", req: " + req);
                        // Parse req string to List<AccountRebalanceCriteria>
                        List<AccountRebalanceCriteria> criterias = parseCriteriasFromString(req);
                        System.out.println("runRebalanceAsync: criterias size = " + criterias.size());
                        StringBuilder whereClause = new StringBuilder();
                        List<Object> params = new ArrayList<>();
                        for (int i = 0; i < criterias.size(); i++) {
                            AccountRebalanceCriteria c = criterias.get(i);
                            String attr = c.getAttribute().replaceAll("_", "").toLowerCase();
                            String op = c.getOperator();
                            String val = c.getValue();
                            whereClause.append(attr).append(" ").append(op).append(" ?");
                            if (i < criterias.size() - 1) whereClause.append(" AND ");
                            // Try to parse as number, else treat as string
                            if (val.matches("^-?\\d+(\\.\\d+)?$")) {
                                params.add(Double.valueOf(val));
                            } else {
                                params.add(val);
                            }
                        }
                        String sql = "SELECT * FROM accounts WHERE " + whereClause;
                        List<Map<String, Object>> results = new ArrayList<>();
                        try (Connection accConn = DriverManager.getConnection("jdbc:h2:file:./data/upload-db", "sa", "")) {
                            try (PreparedStatement accPs = accConn.prepareStatement(sql)) {
                                for (int i = 0; i < params.size(); i++) {
                                    Object param = params.get(i);
                                    if (param instanceof Double) {
                                        accPs.setDouble(i + 1, (Double) param);
                                    } else {
                                        accPs.setString(i + 1, param.toString());
                                    }
                                }
                                try (ResultSet accRs = accPs.executeQuery()) {
                                    ResultSetMetaData meta = accRs.getMetaData();
                                    int colCount = meta.getColumnCount();
                                    while (accRs.next()) {
                                        Map<String, Object> row = new LinkedHashMap<>();
                                        for (int i = 1; i <= colCount; i++) {
                                            row.put(meta.getColumnName(i), accRs.getObject(i));
                                        }
                                        results.add(row);
                                        System.out.println(row);
                                    }
                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        // Update processflg1 to 'Y' for current reqid
                        try (Connection updateConn = DriverManager.getConnection(rebalDbUrl, "sa", "")) {
                            try (PreparedStatement updatePs = updateConn.prepareStatement("UPDATE reqs SET processflg1 = ? WHERE reqid = ?")) {
                                updatePs.setBoolean(1, true);
                                updatePs.setString(2, reqid);
                                updatePs.executeUpdate();
                                System.out.println("Updated processflg1 to true for reqid: " + reqid);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Helper to parse criterias from string (stub, needs real implementation)
    private List<AccountRebalanceCriteria> parseCriteriasFromString(String req) {
        // Deserialize JSON string to List<AccountRebalanceCriteria>
        Gson gson = new Gson();
        AccountRebalanceCriteria[] arr = gson.fromJson(req, AccountRebalanceCriteria[].class);
        List<AccountRebalanceCriteria> criterias = Arrays.asList(arr);
        // Example usage of getters
        for (AccountRebalanceCriteria c : criterias) {
            System.out.println("attribute: " + c.getAttribute() + ", operator: " + c.getOperator() + ", value: " + c.getValue());
        }
        return criterias;
    }

}
