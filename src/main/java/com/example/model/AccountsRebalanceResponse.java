package com.example.model;

import java.util.List;
import java.util.Map;

public class AccountsRebalanceResponse {
    private List<Map<String, Object>> accounts;

    public AccountsRebalanceResponse(List<Map<String, Object>> accounts) {
        this.accounts = accounts;
    }
    public List<Map<String, Object>> getAccounts() { return accounts; }
    public void setAccounts(List<Map<String, Object>> accounts) { this.accounts = accounts; }
}
