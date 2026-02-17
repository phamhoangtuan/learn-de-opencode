package com.learnde.pipeline.rules;

import com.learnde.pipeline.models.Alert;
import com.learnde.pipeline.models.Transaction;

import java.time.Instant;
import java.util.Optional;

/**
 * RapidActivityRule — flags accounts with excessive transaction frequency.
 *
 * <p>Rule type: velocity. Default: 5 transactions within 1 minute window.
 * Generates a "medium" severity alert when triggered.
 *
 * <p>Note: This rule's stateful evaluation (tracking per-account counts over a
 * time window) is handled by the AlertEvaluator KeyedProcessFunction using
 * Flink ValueState and event-time timers. This class provides the rule
 * configuration and alert construction. The actual windowed counting logic
 * lives in AlertEvaluator.
 *
 * <p>The evaluate() method here serves as a simple threshold check given a
 * pre-computed count, which the AlertEvaluator calls after updating state.
 */
public class RapidActivityRule implements AlertingRule {

    private static final long serialVersionUID = 1L;

    private final int maxCount;
    private final int windowMinutes;
    private final String severity;
    private final boolean enabled;

    /** Constructs a RapidActivityRule. @param maxCount maximum transactions allowed in window @param windowMinutes window duration in minutes @param severity alert severity level @param enabled whether this rule is active */
    public RapidActivityRule(int maxCount, int windowMinutes,
                             String severity, boolean enabled) {
        this.maxCount = maxCount;
        this.windowMinutes = windowMinutes;
        this.severity = severity;
        this.enabled = enabled;
    }

    /** Default constructor with 5 tx / 1 minute window. */
    public RapidActivityRule() {
        this(5, 1, "medium", true);
    }

    /** {@inheritDoc} */
    @Override
    public String getRuleName() {
        return "rapid-activity";
    }

    /** {@inheritDoc} */
    @Override
    public String getSeverity() {
        return severity;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isEnabled() {
        return enabled;
    }

    /** @return maximum transaction count allowed in window */
    public int getMaxCount() {
        return maxCount;
    }

    /** @return window duration in minutes */
    public int getWindowMinutes() {
        return windowMinutes;
    }

    /**
     * Evaluate whether the given count within the window exceeds the threshold.
     *
     * @param transaction the transaction that caused the threshold breach
     * @param currentCount the number of transactions in the current window
     * @return an Optional containing an Alert if threshold exceeded
     */
    public Optional<Alert> evaluateCount(Transaction transaction, int currentCount) {
        if (!enabled) {
            return Optional.empty();
        }

        if (currentCount > maxCount) {
            String description = String.format(
                    "Account %s produced %d transactions within %d-minute window",
                    transaction.getAccountId(), currentCount, windowMinutes);
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

    /**
     * Stateless evaluate — always returns empty because velocity detection
     * requires state (handled by AlertEvaluator).
     *
     * @param transaction the transaction to evaluate
     * @return always empty — use evaluateCount() with state instead
     */
    @Override
    public Optional<Alert> evaluate(Transaction transaction) {
        // Velocity detection requires state; see evaluateCount()
        return Optional.empty();
    }
}
