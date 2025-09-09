package com.example;

import java.util.List;

public class RebalanceRequest {
    private String requestIdentifier;
    private List<AccountRebalanceCriteria> accountRebalanceCriterias;

    public RebalanceRequest() {}
    public RebalanceRequest(String requestIdentifier, List<AccountRebalanceCriteria> criterias) {
        this.requestIdentifier = requestIdentifier;
        this.accountRebalanceCriterias = criterias;
    }
    public String getRequestIdentifier() { return requestIdentifier; }
    public void setRequestIdentifier(String requestIdentifier) { this.requestIdentifier = requestIdentifier; }
    public List<AccountRebalanceCriteria> getAccountRebalanceCriterias() { return accountRebalanceCriterias; }
    public void setAccountRebalanceCriterias(List<AccountRebalanceCriteria> criterias) { this.accountRebalanceCriterias = criterias; }
}
