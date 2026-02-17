package com.learnde.pipeline.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;

/**
 * Transaction POJO — represents a financial transaction flowing through the pipeline.
 *
 * <p>Deserialized from JSON on the Kafka raw-transactions topic. Enriched by Flink with
 * processing_timestamp, partition_date, and is_flagged before writing to Iceberg.
 *
 * <p>Jackson annotations enable JSON deserialization. Implements Serializable for
 * Flink state management and checkpointing.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Transaction implements Serializable {

    private static final long serialVersionUID = 1L;

    @JsonProperty("transaction_id")
    private String transactionId;

    @JsonProperty("timestamp")
    private String timestamp;

    @JsonProperty("account_id")
    private String accountId;

    @JsonProperty("amount")
    private BigDecimal amount;

    @JsonProperty("currency")
    private String currency;

    @JsonProperty("merchant_name")
    private String merchantName;

    @JsonProperty("merchant_category")
    private String merchantCategory;

    @JsonProperty("transaction_type")
    private String transactionType;

    @JsonProperty("location_country")
    private String locationCountry;

    @JsonProperty("status")
    private String status;

    // Enrichment fields added by Flink
    @JsonProperty("processing_timestamp")
    private String processingTimestamp;

    @JsonProperty("partition_date")
    private String partitionDate;

    @JsonProperty("is_flagged")
    private boolean isFlagged;

    /** No-arg constructor required by Jackson deserialization. */
    public Transaction() {}

    // --- Getters and Setters ---

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getAccountId() {
        return accountId;
    }

    public void setAccountId(String accountId) {
        this.accountId = accountId;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public String getMerchantName() {
        return merchantName;
    }

    public void setMerchantName(String merchantName) {
        this.merchantName = merchantName;
    }

    public String getMerchantCategory() {
        return merchantCategory;
    }

    public void setMerchantCategory(String merchantCategory) {
        this.merchantCategory = merchantCategory;
    }

    public String getTransactionType() {
        return transactionType;
    }

    public void setTransactionType(String transactionType) {
        this.transactionType = transactionType;
    }

    public String getLocationCountry() {
        return locationCountry;
    }

    public void setLocationCountry(String locationCountry) {
        this.locationCountry = locationCountry;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getProcessingTimestamp() {
        return processingTimestamp;
    }

    public void setProcessingTimestamp(String processingTimestamp) {
        this.processingTimestamp = processingTimestamp;
    }

    public String getPartitionDate() {
        return partitionDate;
    }

    public void setPartitionDate(String partitionDate) {
        this.partitionDate = partitionDate;
    }

    public boolean isFlagged() {
        return isFlagged;
    }

    public void setFlagged(boolean flagged) {
        isFlagged = flagged;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Transaction that = (Transaction) o;
        return Objects.equals(transactionId, that.transactionId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(transactionId);
    }

    @Override
    public String toString() {
        return "Transaction{"
                + "transactionId='" + transactionId + '\''
                + ", accountId='" + accountId + '\''
                + ", amount=" + amount
                + ", currency='" + currency + '\''
                + ", type='" + transactionType + '\''
                + ", flagged=" + isFlagged
                + '}';
    }
}
