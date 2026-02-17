package com.learnde.pipeline.models;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

/**
 * Alert POJO — represents an alert generated when a transaction matches an alerting rule.
 *
 * <p>Published as JSON to the Kafka alerts topic and persisted to the Iceberg alerts table.
 * Each alert references a single transaction and the rule that triggered it.
 * A single transaction may generate multiple alerts if it matches multiple rules.
 */
public class Alert implements Serializable {

    private static final long serialVersionUID = 1L;

    @JsonProperty("alert_id")
    private String alertId;

    @JsonProperty("transaction_id")
    private String transactionId;

    @JsonProperty("rule_name")
    private String ruleName;

    @JsonProperty("severity")
    private String severity;

    @JsonProperty("alert_timestamp")
    private String alertTimestamp;

    @JsonProperty("description")
    private String description;

    /** No-arg constructor required by Jackson. */
    public Alert() {}

    /**
     * Convenience constructor for creating a fully populated alert.
     */
    public Alert(String transactionId, String ruleName, String severity,
                 String alertTimestamp, String description) {
        this.alertId = UUID.randomUUID().toString();
        this.transactionId = transactionId;
        this.ruleName = ruleName;
        this.severity = severity;
        this.alertTimestamp = alertTimestamp;
        this.description = description;
    }

    // --- Getters and Setters ---

    public String getAlertId() {
        return alertId;
    }

    public void setAlertId(String alertId) {
        this.alertId = alertId;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public String getRuleName() {
        return ruleName;
    }

    public void setRuleName(String ruleName) {
        this.ruleName = ruleName;
    }

    public String getSeverity() {
        return severity;
    }

    public void setSeverity(String severity) {
        this.severity = severity;
    }

    public String getAlertTimestamp() {
        return alertTimestamp;
    }

    public void setAlertTimestamp(String alertTimestamp) {
        this.alertTimestamp = alertTimestamp;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Alert alert = (Alert) o;
        return Objects.equals(alertId, alert.alertId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(alertId);
    }

    @Override
    public String toString() {
        return "Alert{"
                + "alertId='" + alertId + '\''
                + ", transactionId='" + transactionId + '\''
                + ", ruleName='" + ruleName + '\''
                + ", severity='" + severity + '\''
                + '}';
    }
}
