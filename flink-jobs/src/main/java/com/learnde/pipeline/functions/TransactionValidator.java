package com.learnde.pipeline.functions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnde.pipeline.models.DeadLetterRecord;
import com.learnde.pipeline.models.Transaction;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Validates incoming transactions against data-model.md rules.
 *
 * <p>Valid transactions are emitted to the main output. Invalid transactions
 * are routed to the dead-letter side output with error details.
 *
 * <p>Validation rules:
 * <ul>
 *   <li>transaction_id: UUID format</li>
 *   <li>timestamp: valid ISO 8601, not future by more than 1 minute</li>
 *   <li>account_id: alphanumeric, 10-12 chars</li>
 *   <li>amount: non-zero, range [-50000, 50000]</li>
 *   <li>currency: one of USD, EUR, GBP, JPY</li>
 *   <li>merchant_name: non-empty, max 100 chars</li>
 *   <li>merchant_category: valid enum value</li>
 *   <li>transaction_type: valid enum value</li>
 *   <li>location_country: 2-letter uppercase</li>
 *   <li>status: valid enum value</li>
 * </ul>
 */
public class TransactionValidator extends ProcessFunction<Transaction, Transaction> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(TransactionValidator.class);

    /** Side output tag for dead-letter records. */
    public static final OutputTag<DeadLetterRecord> DLQ_TAG =
            new OutputTag<DeadLetterRecord>("dead-letter") {};

    private static final Set<String> VALID_CURRENCIES =
            Set.of("USD", "EUR", "GBP", "JPY");
    private static final Set<String> VALID_CATEGORIES =
            Set.of("retail", "dining", "travel", "online", "groceries", "entertainment", "utilities");
    private static final Set<String> VALID_TYPES =
            Set.of("purchase", "withdrawal", "transfer", "refund");
    private static final Set<String> VALID_STATUSES =
            Set.of("pending", "completed", "failed", "reversed");
    private static final BigDecimal MAX_AMOUNT = new BigDecimal("50000");
    private static final BigDecimal MIN_AMOUNT = new BigDecimal("-50000");

    private transient ObjectMapper objectMapper;
    private final String processorId;

    public TransactionValidator(String processorId) {
        this.processorId = processorId;
    }

    public TransactionValidator() {
        this("transaction-validator-0");
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) {
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public void processElement(Transaction tx, Context ctx, Collector<Transaction> out)
            throws Exception {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
        }

        // Validate each field; emit to DLQ on first failure
        String errorField = null;
        String errorType = null;
        String expectedFormat = null;
        String actualValue = null;

        // transaction_id: required, UUID format
        if (tx.getTransactionId() == null || tx.getTransactionId().isEmpty()) {
            errorField = "transaction_id";
            errorType = "missing";
            expectedFormat = "UUID v4 format";
            actualValue = null;
        } else if (!isValidUuid(tx.getTransactionId())) {
            errorField = "transaction_id";
            errorType = "invalid_format";
            expectedFormat = "UUID v4 format";
            actualValue = tx.getTransactionId();
        }

        // timestamp: required, ISO 8601, not future >1min
        if (errorField == null) {
            if (tx.getTimestamp() == null || tx.getTimestamp().isEmpty()) {
                errorField = "timestamp";
                errorType = "missing";
                expectedFormat = "ISO 8601 datetime with timezone";
                actualValue = null;
            } else {
                try {
                    ZonedDateTime txTime = ZonedDateTime.parse(tx.getTimestamp());
                    Instant txInstant = txTime.toInstant();
                    if (txInstant.isAfter(Instant.now().plusSeconds(60))) {
                        errorField = "timestamp";
                        errorType = "out_of_range";
                        expectedFormat = "ISO 8601 datetime not more than 1 minute in the future";
                        actualValue = tx.getTimestamp();
                    }
                } catch (DateTimeParseException e) {
                    errorField = "timestamp";
                    errorType = "invalid_format";
                    expectedFormat = "ISO 8601 datetime with timezone";
                    actualValue = tx.getTimestamp();
                }
            }
        }

        // account_id: required, alphanumeric 10-12 chars
        if (errorField == null) {
            if (tx.getAccountId() == null || tx.getAccountId().isEmpty()) {
                errorField = "account_id";
                errorType = "missing";
                expectedFormat = "alphanumeric string, 10-12 characters";
                actualValue = null;
            } else if (!tx.getAccountId().matches("^[a-zA-Z0-9]{10,12}$")) {
                errorField = "account_id";
                errorType = "invalid_format";
                expectedFormat = "alphanumeric string, 10-12 characters";
                actualValue = tx.getAccountId();
            }
        }

        // amount: required, non-zero, range [-50000, 50000]
        if (errorField == null) {
            if (tx.getAmount() == null) {
                errorField = "amount";
                errorType = "missing";
                expectedFormat = "non-zero decimal in range [-50000, 50000]";
                actualValue = null;
            } else if (tx.getAmount().compareTo(BigDecimal.ZERO) == 0) {
                errorField = "amount";
                errorType = "out_of_range";
                expectedFormat = "non-zero decimal in range [-50000, 50000]";
                actualValue = tx.getAmount().toString();
            } else if (tx.getAmount().compareTo(MAX_AMOUNT) > 0
                    || tx.getAmount().compareTo(MIN_AMOUNT) < 0) {
                errorField = "amount";
                errorType = "out_of_range";
                expectedFormat = "non-zero decimal in range [-50000, 50000]";
                actualValue = tx.getAmount().toString();
            }
        }

        // currency: required, valid enum
        if (errorField == null) {
            if (tx.getCurrency() == null || tx.getCurrency().isEmpty()) {
                errorField = "currency";
                errorType = "missing";
                expectedFormat = "one of: USD, EUR, GBP, JPY";
                actualValue = null;
            } else if (!VALID_CURRENCIES.contains(tx.getCurrency())) {
                errorField = "currency";
                errorType = "invalid_enum";
                expectedFormat = "one of: USD, EUR, GBP, JPY";
                actualValue = tx.getCurrency();
            }
        }

        // merchant_name: required, non-empty, max 100 chars
        if (errorField == null) {
            if (tx.getMerchantName() == null || tx.getMerchantName().isEmpty()) {
                errorField = "merchant_name";
                errorType = "missing";
                expectedFormat = "non-empty string, max 100 characters";
                actualValue = tx.getMerchantName();
            } else if (tx.getMerchantName().length() > 100) {
                errorField = "merchant_name";
                errorType = "out_of_range";
                expectedFormat = "non-empty string, max 100 characters";
                actualValue = tx.getMerchantName();
            }
        }

        // merchant_category: required, valid enum
        if (errorField == null) {
            if (tx.getMerchantCategory() == null || tx.getMerchantCategory().isEmpty()) {
                errorField = "merchant_category";
                errorType = "missing";
                expectedFormat = "one of: retail, dining, travel, online, groceries, entertainment, utilities";
                actualValue = null;
            } else if (!VALID_CATEGORIES.contains(tx.getMerchantCategory())) {
                errorField = "merchant_category";
                errorType = "invalid_enum";
                expectedFormat = "one of: retail, dining, travel, online, groceries, entertainment, utilities";
                actualValue = tx.getMerchantCategory();
            }
        }

        // transaction_type: required, valid enum
        if (errorField == null) {
            if (tx.getTransactionType() == null || tx.getTransactionType().isEmpty()) {
                errorField = "transaction_type";
                errorType = "missing";
                expectedFormat = "one of: purchase, withdrawal, transfer, refund";
                actualValue = null;
            } else if (!VALID_TYPES.contains(tx.getTransactionType())) {
                errorField = "transaction_type";
                errorType = "invalid_enum";
                expectedFormat = "one of: purchase, withdrawal, transfer, refund";
                actualValue = tx.getTransactionType();
            }
        }

        // location_country: required, 2-letter uppercase
        if (errorField == null) {
            if (tx.getLocationCountry() == null || tx.getLocationCountry().isEmpty()) {
                errorField = "location_country";
                errorType = "missing";
                expectedFormat = "2-letter ISO 3166-1 alpha-2 country code";
                actualValue = null;
            } else if (!tx.getLocationCountry().matches("^[A-Z]{2}$")) {
                errorField = "location_country";
                errorType = "invalid_format";
                expectedFormat = "2-letter ISO 3166-1 alpha-2 country code";
                actualValue = tx.getLocationCountry();
            }
        }

        // status: required, valid enum
        if (errorField == null) {
            if (tx.getStatus() == null || tx.getStatus().isEmpty()) {
                errorField = "status";
                errorType = "missing";
                expectedFormat = "one of: pending, completed, failed, reversed";
                actualValue = null;
            } else if (!VALID_STATUSES.contains(tx.getStatus())) {
                errorField = "status";
                errorType = "invalid_enum";
                expectedFormat = "one of: pending, completed, failed, reversed";
                actualValue = tx.getStatus();
            }
        }

        if (errorField != null) {
            // Route to dead-letter
            String originalJson;
            try {
                originalJson = objectMapper.writeValueAsString(tx);
            } catch (Exception e) {
                originalJson = "{}";
            }

            DeadLetterRecord dlr = new DeadLetterRecord(
                    originalJson,
                    errorField,
                    errorType,
                    expectedFormat,
                    actualValue,
                    Instant.now().toString(),
                    processorId);

            ctx.output(DLQ_TAG, dlr);
            LOG.warn("Transaction validation failed: field={}, type={}, value={}",
                    errorField, errorType, actualValue);
        } else {
            // Valid — enrich and emit
            tx.setProcessingTimestamp(Instant.now().toString());
            try {
                ZonedDateTime txTime = ZonedDateTime.parse(tx.getTimestamp());
                tx.setPartitionDate(txTime.toLocalDate().toString());
            } catch (DateTimeParseException e) {
                tx.setPartitionDate(java.time.LocalDate.now().toString());
            }
            out.collect(tx);
        }
    }

    private boolean isValidUuid(String value) {
        try {
            UUID.fromString(value);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }
}
