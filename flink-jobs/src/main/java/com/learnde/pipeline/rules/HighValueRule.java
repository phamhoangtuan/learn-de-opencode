package com.learnde.pipeline.rules;

import com.learnde.pipeline.models.Alert;
import com.learnde.pipeline.models.Transaction;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;

/**
 * HighValueRule — flags transactions with amount exceeding a configurable threshold.
 *
 * <p>Rule type: threshold. Default threshold: $10,000.
 * Generates a "high" severity alert when triggered.
 */
public class HighValueRule implements AlertingRule {

    private static final long serialVersionUID = 1L;

    private final BigDecimal amountThreshold;
    private final String severity;
    private final boolean enabled;

    public HighValueRule(BigDecimal amountThreshold, String severity, boolean enabled) {
        this.amountThreshold = amountThreshold;
        this.severity = severity;
        this.enabled = enabled;
    }

    /** Default constructor with $10,000 threshold. */
    public HighValueRule() {
        this(new BigDecimal("10000"), "high", true);
    }

    @Override
    public String getRuleName() {
        return "high-value-transaction";
    }

    @Override
    public String getSeverity() {
        return severity;
    }

    @Override
    public boolean isEnabled() {
        return enabled;
    }

    public BigDecimal getAmountThreshold() {
        return amountThreshold;
    }

    @Override
    public Optional<Alert> evaluate(Transaction transaction) {
        if (!enabled || transaction.getAmount() == null) {
            return Optional.empty();
        }

        // Compare absolute value against threshold
        if (transaction.getAmount().abs().compareTo(amountThreshold) > 0) {
            String description = String.format(
                    "Transaction amount $%,.2f exceeds threshold of $%,.2f",
                    transaction.getAmount().abs(), amountThreshold);
            Alert alert = new Alert(
                    transaction.getTransactionId(),
                    getRuleName(),
                    severity,
                    Instant.now().toString(),
                    description);
            return Optional.of(alert);
        }

        return Optional.empty();
    }
}
