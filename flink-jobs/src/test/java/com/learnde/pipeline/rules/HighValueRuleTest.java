package com.learnde.pipeline.rules;

import com.learnde.pipeline.models.Alert;
import com.learnde.pipeline.models.Transaction;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * T041: Unit tests for HighValueRule — threshold detection on transaction amount.
 */
class HighValueRuleTest {

    private HighValueRule rule;

    @BeforeEach
    void setUp() {
        rule = new HighValueRule(); // Default: $10,000 threshold
    }

    private Transaction createTransaction(BigDecimal amount) {
        Transaction tx = new Transaction();
        tx.setTransactionId("550e8400-e29b-41d4-a716-446655440000");
        tx.setTimestamp("2026-02-16T14:30:00.000Z");
        tx.setAccountId("ACC123456789");
        tx.setAmount(amount);
        tx.setCurrency("USD");
        return tx;
    }

    @Test
    @DisplayName("Transaction exceeding threshold triggers alert")
    void test_evaluate_exceedsThreshold_triggersAlert() {
        Transaction tx = createTransaction(new BigDecimal("12500.00"));
        Optional<Alert> result = rule.evaluate(tx);

        assertThat(result).isPresent();
        assertThat(result.get().getRuleName()).isEqualTo("high-value-transaction");
        assertThat(result.get().getSeverity()).isEqualTo("high");
        assertThat(result.get().getTransactionId()).isEqualTo(tx.getTransactionId());
    }

    @Test
    @DisplayName("Transaction at exactly threshold does not trigger alert")
    void test_evaluate_exactlyAtThreshold_noAlert() {
        Transaction tx = createTransaction(new BigDecimal("10000.00"));
        Optional<Alert> result = rule.evaluate(tx);

        assertThat(result).isEmpty();
    }

    @Test
    @DisplayName("Transaction below threshold does not trigger alert")
    void test_evaluate_belowThreshold_noAlert() {
        Transaction tx = createTransaction(new BigDecimal("5000.00"));
        Optional<Alert> result = rule.evaluate(tx);

        assertThat(result).isEmpty();
    }

    @Test
    @DisplayName("Large negative amount (absolute value) triggers alert")
    void test_evaluate_largeNegativeAmount_triggersAlert() {
        Transaction tx = createTransaction(new BigDecimal("-15000.00"));
        Optional<Alert> result = rule.evaluate(tx);

        assertThat(result).isPresent();
        assertThat(result.get().getRuleName()).isEqualTo("high-value-transaction");
    }

    @Test
    @DisplayName("Custom threshold value is respected")
    void test_evaluate_customThreshold_respected() {
        HighValueRule customRule = new HighValueRule(new BigDecimal("5000"), "critical", true);
        Transaction tx = createTransaction(new BigDecimal("6000.00"));
        Optional<Alert> result = customRule.evaluate(tx);

        assertThat(result).isPresent();
        assertThat(result.get().getSeverity()).isEqualTo("critical");
    }

    @Test
    @DisplayName("Disabled rule returns empty")
    void test_evaluate_disabledRule_returnsEmpty() {
        HighValueRule disabledRule = new HighValueRule(new BigDecimal("10000"), "high", false);
        Transaction tx = createTransaction(new BigDecimal("50000.00"));
        Optional<Alert> result = disabledRule.evaluate(tx);

        assertThat(result).isEmpty();
    }

    @Test
    @DisplayName("Null amount returns empty")
    void test_evaluate_nullAmount_returnsEmpty() {
        Transaction tx = createTransaction(null);
        Optional<Alert> result = rule.evaluate(tx);

        assertThat(result).isEmpty();
    }

    @Test
    @DisplayName("Alert description includes amount and threshold")
    void test_evaluate_alertDescription_includesDetails() {
        Transaction tx = createTransaction(new BigDecimal("12500.00"));
        Optional<Alert> result = rule.evaluate(tx);

        assertThat(result).isPresent();
        assertThat(result.get().getDescription()).contains("12,500.00");
        assertThat(result.get().getDescription()).contains("10,000");
    }

    @Test
    @DisplayName("Rule name is high-value-transaction")
    void test_getRuleName_returnsCorrectName() {
        assertThat(rule.getRuleName()).isEqualTo("high-value-transaction");
    }

    @Test
    @DisplayName("Alert has unique alert_id")
    void test_evaluate_alertHasUniqueId() {
        Transaction tx = createTransaction(new BigDecimal("20000.00"));
        Optional<Alert> result1 = rule.evaluate(tx);
        Optional<Alert> result2 = rule.evaluate(tx);

        assertThat(result1).isPresent();
        assertThat(result2).isPresent();
        assertThat(result1.get().getAlertId()).isNotEqualTo(result2.get().getAlertId());
    }
}
