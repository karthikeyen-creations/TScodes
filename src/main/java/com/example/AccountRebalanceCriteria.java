package com.example;

public class AccountRebalanceCriteria {
    private String attribute;
    private String operator;
    private String value;

    public AccountRebalanceCriteria() {}
    public AccountRebalanceCriteria(String attribute, String operator, String value) {
        this.attribute = attribute;
        this.operator = operator;
        this.value = value;
    }
    public String getAttribute() { return attribute; }
    public void setAttribute(String attribute) { this.attribute = attribute; }
    public String getOperator() { return operator; }
    public void setOperator(String operator) { this.operator = operator; }
    public String getValue() { return value; }
    public void setValue(String value) { this.value = value; }
    @Override
    public String toString() {
        return "AccountRebalanceCriteria{" +
                "attribute='" + attribute + '\'' +
                ", operator='" + operator + '\'' +
                ", value='" + value + '\'' +
                '}';
    }
}
