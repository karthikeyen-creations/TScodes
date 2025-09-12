
package com.example.service;

import java.sql.*;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

import com.example.model.AccountRebalanceCriteria;
import com.example.model.RebalanceRequest;
import com.google.gson.Gson;

public class RebalanceHandler {
    public String handleAsyncRebalRequest(RebalanceRequest rebalRequest) {
    String rebalDbUrl = "jdbc:h2:file:./data/RebalReqs";
        // Write rebalRequest to RebalReqs DB
        String reqid = rebalRequest.getRequestIdentifier();
        String req = new Gson().toJson(rebalRequest.getAccountRebalanceCriterias());
        try (Connection conn = DriverManager.getConnection(rebalDbUrl, "sa", "")) {
            JdbcTemplate jdbc = new JdbcTemplate(new SingleConnectionDataSource(conn, true));
            jdbc.update("INSERT INTO reqs (reqid, req, processflg1, processflg2) VALUES (?, ?, ?, ?)", reqid, req, false, false);
        } catch (Exception e) {
            e.printStackTrace();
        }
        // Only call runRebalanceAsync if ordertailoring process is not running
        boolean canRun = false;
        // String rebalDbUrl = "jdbc:h2:file:./data/RebalReqs";
        try (Connection conn = DriverManager.getConnection(rebalDbUrl, "sa", "")) {
            try (PreparedStatement ps = conn.prepareStatement("SELECT running FROM processes WHERE process = ?")) {
                ps.setString(1, "ordertailoring");
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        boolean running = rs.getBoolean("running");
                        if (!running) {
                            canRun = true;
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (canRun) {
            runRebalanceAsync();
        }
        return "done";
    }

    // True async function using a new thread
    public void runRebalanceAsync() {
        System.out.println("runRebalanceAsync: starting new thread");
        new Thread(() -> {
            // Mark ordertailoring process as running
            try (Connection conn = DriverManager.getConnection("jdbc:h2:file:./data/RebalReqs", "sa", "")) {
                try (PreparedStatement ps = conn.prepareStatement("UPDATE processes SET running = ? WHERE process = ?")) {
                    ps.setBoolean(1, true);
                    ps.setString(2, "ordertailoring");
                    ps.executeUpdate();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
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
            // Mark ordertailoring process as not running
            try (Connection conn = DriverManager.getConnection("jdbc:h2:file:./data/RebalReqs", "sa", "")) {
                try (PreparedStatement ps = conn.prepareStatement("UPDATE processes SET running = ? WHERE process = ?")) {
                    ps.setBoolean(1, false);
                    ps.setString(2, "ordertailoring");
                    ps.executeUpdate();
                }
            } catch (Exception e) {
                e.printStackTrace();
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

                            // Process results for exclusions and preferences
                            List<Map<String, Object>> processed = new ArrayList<>();
                            for (Map<String, Object> row : results) {
                                Map<String, Object> entry = new LinkedHashMap<>();
                                entry.put("ACCOUNTID", row.get("ACCOUNTID"));
                                entry.put("accountstatus", row.get("ACCOUNTSTATUS"));

                                // Exclusions and Preferences logic
                                String accountStatus = String.valueOf(row.get("ACCOUNTSTATUS"));
                                List<String> exsec = null, excomp = null, exind = null;
                                String prefsec = null, prefcomp = null, prefind = null;
                                List<Map<String, Object>> holdingsArr = null;
                                Map<String, Object> cashEntry = null;
                                Double holdingstotal = null;
                                List<Map<String, Object>> sellordersArr = null;
                                Double selltotal = null;
                                if ("False".equalsIgnoreCase(accountStatus)) {
                                    exsec = new ArrayList<>();
                                    excomp = new ArrayList<>();
                                    exind = new ArrayList<>();
                                    prefsec = null;
                                    prefcomp = null;
                                    prefind = null;
                                    holdingsArr = new ArrayList<>();
                                    cashEntry = null;
                                    holdingstotal = null;
                                    sellordersArr = new ArrayList<>();
                                    selltotal = null;
                                } else {
                                    String exclusions = String.valueOf(row.get("EXCLUSIONS"));
                                    exsec = new ArrayList<>();
                                    excomp = new ArrayList<>();
                                    exind = new ArrayList<>();
                                    if (exclusions.contains("Exclude Sector:")) {
                                        String[] parts = exclusions.split("Exclude Sector:")[1].split("\\|");
                                        for (String part : parts) {
                                            String sector = part.trim();
                                            if (!sector.isEmpty()) {
                                                exsec.add(sector);
                                            }
                                        }
                                    }
                                    if (exclusions.contains("Exclude Company:")) {
                                        String[] parts = exclusions.split("Exclude Company:")[1].split("\\|");
                                        for (String part : parts) {
                                            // Extract ticker from parentheses if present, else use trimmed part
                                            String ticker = part;
                                            if (part.contains("(")) {
                                                int start = part.indexOf("(") + 1;
                                                int end = part.indexOf(")");
                                                if (start > 0 && end > start) {
                                                    ticker = part.substring(start, end).trim();
                                                }
                                            } else {
                                                ticker = part.trim();
                                            }
                                            excomp.add(ticker);
                                        }
                                    }
                                    if (exclusions.contains("Exclude Industry:")) {
                                        String[] parts = exclusions.split("Exclude Industry:")[1].split("\\|");
                                        for (String part : parts) exind.add(part.trim());
                                    }

                                    String sripref = String.valueOf(row.get("SRIPREFERENCES"));
                                    if (sripref.contains("Prefer ESG:")) {
                                        String[] parts = sripref.split("Prefer ESG:")[1].split("\\(");
                                        if (parts.length > 1) prefcomp = parts[1].replace(")", "").trim();
                                    }
                                    if (sripref.contains("Prefer ESG in:")) {
                                        String secind1 = sripref.split("Prefer ESG in:")[1].trim();
                                        // Split by comma or pipe, handle multiple values
                                        String[] values = secind1.split(",|\\|");
                                        System.out.println("Prefer ESG in values: " + Arrays.toString(values));
                                        for (String val : values) {
                                            String v = val.trim();
                                            if (!v.isEmpty()) {
                                                // Check stocks table for v in gicssubindustry or gicssector
                                                try (PreparedStatement stocksPs = accConn.prepareStatement(
                                                        "SELECT gicssubindustry, gicssector FROM stocks WHERE gicssubindustry = ? OR gicssector = ? LIMIT 1")) {
                                                    stocksPs.setString(1, v);
                                                    stocksPs.setString(2, v);
                                                    try (ResultSet stocksRs = stocksPs.executeQuery()) {
                                                        if (stocksRs.next()) {
                                                            String subindustry = stocksRs.getString("gicssubindustry");
                                                            String sector = stocksRs.getString("gicssector");
                                                            System.out.println("Subindustry: " + subindustry + ", Sector: " + sector);
                                                            if (v.equals(subindustry)) {
                                                                prefind = v;
                                                            }
                                                            if (v.equals(sector)) {
                                                                prefsec = v;
                                                            }
                                                        }
                                                    }
                                                } catch (Exception e) {
                                                    e.printStackTrace();
                                                }
                                            }
                                        }
                                    }

                                    // Populate holdings and cash from holdings table
                                    holdingsArr = new ArrayList<>();
                                    cashEntry = null;
                                    try (PreparedStatement holdPs = accConn.prepareStatement(
                                            "SELECT * FROM holdings WHERE accountid = ?")) {
                                        holdPs.setString(1, String.valueOf(row.get("ACCOUNTID")));
                                        try (ResultSet holdRs = holdPs.executeQuery()) {
                                            holdingstotal = 0.0;
                                            sellordersArr = new ArrayList<>();
                                            selltotal = 0.0;
                                            while (holdRs.next()) {
                                                String ticker = holdRs.getString("ticker");
                                                Map<String, Object> hrow = new LinkedHashMap<>();
                                                ResultSetMetaData hmeta = holdRs.getMetaData();
                                                int hcolCount = hmeta.getColumnCount();
                                                for (int hi = 1; hi <= hcolCount; hi++) {
                                                    hrow.put(hmeta.getColumnName(hi), holdRs.getObject(hi));
                                                }
                                                if ("CASH".equalsIgnoreCase(ticker)) {
                                                    cashEntry = hrow;
                                                } else {
                                                    holdingsArr.add(hrow);
                                                    Object posTotalObj = hrow.get("POSITIONTOTAL");
                                                    if (posTotalObj instanceof Number) {
                                                        holdingstotal += ((Number) posTotalObj).doubleValue();
                                                    } else if (posTotalObj != null) {
                                                        try {
                                                            holdingstotal += Double.parseDouble(posTotalObj.toString());
                                                        } catch (Exception ignore) {}
                                                    }
                                                    // Sellorders logic
                                                    Object sentimentObj = hrow.get("SENTIMENTWEIGHT");
                                                    int sentimentWeight = 0;
                                                    if (sentimentObj instanceof Number) {
                                                        sentimentWeight = ((Number) sentimentObj).intValue();
                                                    } else if (sentimentObj != null) {
                                                        try {
                                                            sentimentWeight = Integer.parseInt(sentimentObj.toString());
                                                        } catch (Exception ignore) {}
                                                    }
                                                    if (sentimentWeight < 0) {
                                                        Map<String, Object> sellorder = new LinkedHashMap<>();
                                                        String accountid = String.valueOf(row.get("ACCOUNTID"));
                                                        sellorder.put("sourceid", accountid + ticker);
                                                        sellorder.put("ticker", ticker);
                                                        sellorder.put("side", "S");
                                                        Object qtyObj = hrow.get("QTY");
                                                        sellorder.put("qty", qtyObj);
                                                        sellordersArr.add(sellorder);
                                                        // Add to selltotal
                                                        if (posTotalObj instanceof Number) {
                                                            selltotal += ((Number) posTotalObj).doubleValue();
                                                        } else if (posTotalObj != null) {
                                                            try {
                                                                selltotal += Double.parseDouble(posTotalObj.toString());
                                                            } catch (Exception ignore) {}
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    }
                                }
                                entry.put("exsec", exsec);
                                entry.put("excomp", excomp);
                                entry.put("exind", exind);
                                entry.put("prefsec", prefsec);
                                entry.put("prefcomp", prefcomp);
                                entry.put("prefind", prefind);
                                entry.put("holdings", holdingsArr);
                                Object cashValue = "0";
                                if (cashEntry != null && cashEntry.get("POSITIONTOTAL") != null) {
                                    cashValue = cashEntry.get("POSITIONTOTAL");
                                }
                                entry.put("cash", cashValue);
                                entry.put("holdingstotal", holdingstotal);
                                entry.put("sellorders", sellordersArr);
                                entry.put("selltotal", selltotal);

                                // Add prefstocks array
                                List<Map<String, Object>> prefstocksArr = null;
                                if ("False".equalsIgnoreCase(accountStatus)) {
                                    prefstocksArr = new ArrayList<>();
                                } else {
                                    prefstocksArr = new ArrayList<>();
                                    // Build filter conditions
                                    List<String> filterSymbols = prefcomp != null ? Arrays.asList(prefcomp.split(",")) : new ArrayList<>();
                                    String filterSector = prefsec != null ? prefsec : null;
                                    String filterSubindustry = prefind != null ? prefind : null;
                                    // Query stocks table
                                    try (PreparedStatement stocksPs = accConn.prepareStatement(
                                            "SELECT * FROM stocks ORDER BY sentimentweight DESC")) {
                                        try (ResultSet stocksRs = stocksPs.executeQuery()) {
                                            ResultSetMetaData smeta = stocksRs.getMetaData();
                                            int scolCount = smeta.getColumnCount();
                                            while (stocksRs.next()) {
                                                String symbol = stocksRs.getString("symbol");
                                                String gicssector = stocksRs.getString("gicssector");
                                                String gicssubindustry = stocksRs.getString("gicssubindustry");
                                                boolean matches = false;
                                                if (filterSymbols != null && !filterSymbols.isEmpty()) {
                                                    for (String fs : filterSymbols) {
                                                        if (symbol != null && symbol.equalsIgnoreCase(fs.trim())) {
                                                            matches = true;
                                                            break;
                                                        }
                                                    }
                                                }
                                                if (!matches && filterSector != null && gicssector != null && gicssector.equalsIgnoreCase(filterSector)) {
                                                    matches = true;
                                                }
                                                if (!matches && filterSubindustry != null && gicssubindustry != null && gicssubindustry.equalsIgnoreCase(filterSubindustry)) {
                                                    matches = true;
                                                }
                                                if (matches) {
                                                    int sentimentWeight = 0;
                                                    Object swObj = stocksRs.getObject("sentimentweight");
                                                    if (swObj instanceof Number) {
                                                        sentimentWeight = ((Number) swObj).intValue();
                                                    } else if (swObj != null) {
                                                        try {
                                                            sentimentWeight = Integer.parseInt(swObj.toString());
                                                        } catch (Exception ignore) {}
                                                    }
                                                    if (sentimentWeight > 0) {
                                                        Map<String, Object> srow = new LinkedHashMap<>();
                                                        for (int si = 1; si <= scolCount; si++) {
                                                            srow.put(smeta.getColumnName(si), stocksRs.getObject(si));
                                                        }
                                                        prefstocksArr.add(srow);
                                                    }
                                                }
                                            }
                                        }
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    }
                                }
                                entry.put("prefstocks", prefstocksArr);

                                // Add prefstockscount and cashaftersell
                                Object prefstockscount = null;
                                Object cashaftersell = null;
                                if ("False".equalsIgnoreCase(accountStatus)) {
                                    prefstockscount = null;
                                    cashaftersell = null;
                                } else {
                                    prefstockscount = (prefstocksArr != null) ? prefstocksArr.size() : 0;
                                    // cashValue may be string or number
                                    double cashVal = 0.0;
                                    if (cashValue != null) {
                                        try {
                                            cashVal = Double.parseDouble(cashValue.toString());
                                        } catch (Exception ignore) {}
                                    }
                                    double sellTotalVal = (selltotal != null) ? selltotal : 0.0;
                                    cashaftersell = cashVal + sellTotalVal;
                                }
                                entry.put("prefstockscount", prefstockscount);
                                entry.put("cashaftersell", cashaftersell);

                                // Add otherpositivestocks array
                                List<Map<String, Object>> otherpositivestocksArr = null;
                                if ("False".equalsIgnoreCase(accountStatus)) {
                                    otherpositivestocksArr = new ArrayList<>();
                                } else {
                                    otherpositivestocksArr = new ArrayList<>();
                                    // Prepare exclusion and preference sets for fast lookup
                                    Set<String> prefcompSet = prefcomp != null ? new HashSet<>(Arrays.asList(prefcomp.split(","))) : new HashSet<>();
                                    Set<String> excompSet = excomp != null ? new HashSet<>(excomp) : new HashSet<>();
                                    Set<String> prefsecSet = prefsec != null ? new HashSet<>(Arrays.asList(prefsec.split(","))) : new HashSet<>();
                                    Set<String> exsecSet = exsec != null ? new HashSet<>(exsec) : new HashSet<>();
                                    Set<String> prefindSet = prefind != null ? new HashSet<>(Arrays.asList(prefind.split(","))) : new HashSet<>();
                                    Set<String> exindSet = exind != null ? new HashSet<>(exind) : new HashSet<>();
                                    // Query stocks table
                                    try (PreparedStatement stocksPs = accConn.prepareStatement(
                                            "SELECT * FROM stocks ORDER BY sentimentweight DESC")) {
                                        try (ResultSet stocksRs = stocksPs.executeQuery()) {
                                            ResultSetMetaData smeta = stocksRs.getMetaData();
                                            int scolCount = smeta.getColumnCount();
                                            while (stocksRs.next()) {
                                                String symbol = stocksRs.getString("symbol");
                                                String gicssector = stocksRs.getString("gicssector");
                                                String gicssubindustry = stocksRs.getString("gicssubindustry");
                                                int sentimentWeight = 0;
                                                Object swObj = stocksRs.getObject("sentimentweight");
                                                if (swObj instanceof Number) {
                                                    sentimentWeight = ((Number) swObj).intValue();
                                                } else if (swObj != null) {
                                                    try {
                                                        sentimentWeight = Integer.parseInt(swObj.toString());
                                                    } catch (Exception ignore) {}
                                                }
                                                if (sentimentWeight > 0
                                                    && (symbol == null || (!prefcompSet.contains(symbol) && !excompSet.contains(symbol)))
                                                    && (gicssector == null || (!prefsecSet.contains(gicssector) && !exsecSet.contains(gicssector)))
                                                    && (gicssubindustry == null || (!prefindSet.contains(gicssubindustry) && !exindSet.contains(gicssubindustry)))) {
                                                    Map<String, Object> srow = new LinkedHashMap<>();
                                                    for (int si = 1; si <= scolCount; si++) {
                                                        srow.put(smeta.getColumnName(si), stocksRs.getObject(si));
                                                    }
                                                    otherpositivestocksArr.add(srow);
                                                }
                                            }
                                        }
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    }
                                }
                                entry.put("otherpositivestocks", otherpositivestocksArr);

                                // Add otherpositivestockscount
                                Object otherpositivestockscount = null;
                                if ("False".equalsIgnoreCase(accountStatus)) {
                                    otherpositivestockscount = null;
                                } else {
                                    otherpositivestockscount = (otherpositivestocksArr != null) ? otherpositivestocksArr.size() : 0;
                                }
                                entry.put("otherpositivestockscount", otherpositivestockscount);

                                // Add prospectivestocks and prospectivestockscount
                                List<Map<String, Object>> prospectivestocksArr = null;
                                Object prospectivestockscount = null;
                                if ("False".equalsIgnoreCase(accountStatus)) {
                                    prospectivestocksArr = new ArrayList<>();
                                    prospectivestockscount = null;
                                } else {
                                    prospectivestocksArr = new ArrayList<>();
                                    // Add prefstocks first, then otherpositivestocks, up to 50 total
                                    if (prefstocksArr != null) {
                                        for (Map<String, Object> s : prefstocksArr) {
                                            if (prospectivestocksArr.size() < 50) {
                                                prospectivestocksArr.add(s);
                                            } else {
                                                break;
                                            }
                                        }
                                    }
                                    if (otherpositivestocksArr != null && prospectivestocksArr.size() < 50) {
                                        for (Map<String, Object> s : otherpositivestocksArr) {
                                            if (prospectivestocksArr.size() < 50) {
                                                prospectivestocksArr.add(s);
                                            } else {
                                                break;
                                            }
                                        }
                                    }
                                    prospectivestockscount = prospectivestocksArr.size();
                                }
                                entry.put("prospectivestocks", prospectivestocksArr);
                                entry.put("prospectivestockscount", prospectivestockscount);
                                processed.add(entry);
                            }
                            // Print processed array for debug
                            for (Map<String, Object> entry : processed) {
                                System.out.println(entry);
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
