package com.example;

import java.sql.*;
import java.util.*;

public class RebalanceHandler {
    public AccountsRebalanceResponse handleAsyncRebalRequest(RebalanceRequest rebalRequest) {
        List<AccountRebalanceCriteria> criterias = rebalRequest.getAccountRebalanceCriterias();
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
    try (Connection conn = DriverManager.getConnection("jdbc:h2:file:./data/upload-db", "sa", "")) {
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                for (int i = 0; i < params.size(); i++) {
                    Object param = params.get(i);
                    if (param instanceof Double) {
                        ps.setDouble(i + 1, (Double) param);
                    } else {
                        ps.setString(i + 1, param.toString());
                    }
                }
                try (ResultSet rs = ps.executeQuery()) {
                    ResultSetMetaData meta = rs.getMetaData();
                    int colCount = meta.getColumnCount();
                    while (rs.next()) {
                        Map<String, Object> row = new LinkedHashMap<>();
                        for (int i = 1; i <= colCount; i++) {
                            row.put(meta.getColumnName(i), rs.getObject(i));
                        }
                        results.add(row);
                        System.out.println(row);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new AccountsRebalanceResponse(results);
    }
}
