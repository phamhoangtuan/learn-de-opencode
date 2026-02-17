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

    /** @return the transactionId */
    public String getTransactionId() {
        return transactionId;
    }

    /** @param transactionId the transactionId to set */
    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    /** @return the timestamp */
    public String getTimestamp() {
        return timestamp;
    }

    /** @param timestamp the timestamp to set */
    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    /** @return the accountId */
    public String getAccountId() {
        return accountId;
    }

    /** @param accountId the accountId to set */
    public void setAccountId(String accountId) {
        this.accountId = accountId;
    }

    /** @return the amount */
    public BigDecimal getAmount() {
        return amount;
    }

    /** @param amount the amount to set */
    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }

    /** @return the currency */
    public String getCurrency() {
        return currency;
    }

    /** @param currency the currency to set */
    public void setCurrency(String currency) {
        this.currency = currency;
    }

    /** @return the merchantName */
    public String getMerchantName() {
        return merchantName;
    }

    /** @param merchantName the merchantName to set */
    public void setMerchantName(String merchantName) {
        this.merchantName = merchantName;
    }

    /** @return the merchantCategory */
    public String getMerchantCategory() {
        return merchantCategory;
    }

    /** @param merchantCategory the merchantCategory to set */
    public void setMerchantCategory(String merchantCategory) {
        this.merchantCategory = merchantCategory;
    }

    /** @return the transactionType */
    public String getTransactionType() {
        return transactionType;
    }

    /** @param transactionType the transactionType to set */
    public void setTransactionType(String transactionType) {
        this.transactionType = transactionType;
    }

    /** @return the locationCountry */
    public String getLocationCountry() {
        return locationCountry;
    }

    /** @param locationCountry the locationCountry to set */
    public void setLocationCountry(String locationCountry) {
        this.locationCountry = locationCountry;
    }

    /** @return the status */
    public String getStatus() {
        return status;
    }

    /** @param status the status to set */
    public void setStatus(String status) {
        this.status = status;
    }

    /** @return the processingTimestamp */
    public String getProcessingTimestamp() {
        return processingTimestamp;
    }

    /** @param processingTimestamp the processingTimestamp to set */
    public void setProcessingTimestamp(String processingTimestamp) {
        this.processingTimestamp = processingTimestamp;
    }

    /** @return the partitionDate */
    public String getPartitionDate() {
        return partitionDate;
    }

    /** @param partitionDate the partitionDate to set */
    public void setPartitionDate(String partitionDate) {
        this.partitionDate = partitionDate;
    }

    /** @return the isFlagged */
    public boolean isFlagged() {
        return isFlagged;
    }

    /** @param flagged the flagged to set */
    public void setFlagged(boolean flagged) {
        isFlagged = flagged;
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Transaction that = (Transaction) o;
        return Objects.equals(transactionId, that.transactionId);
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        return Objects.hash(transactionId);
    }

    /** {@inheritDoc} */
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
