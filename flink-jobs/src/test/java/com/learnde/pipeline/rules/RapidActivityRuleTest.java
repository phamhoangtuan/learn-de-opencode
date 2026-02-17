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
 * T042: Unit tests for RapidActivityRule — velocity windowed detection.
 *
 * MUST include test case for event-time semantics: send 5 transactions with event
 * timestamps within 1 minute but arriving out of order (processing-time shuffled),
 * verify alert triggers based on event-time window not processing-time (FR-015).
 *
 * Note: The full stateful/windowed behavior is tested in AlertEvaluatorTest using
 * the Flink test harness. This test focuses on the evaluateCount threshold logic.
 */
class RapidActivityRuleTest {

    private RapidActivityRule rule;

    @BeforeEach
    void setUp() {
        rule = new RapidActivityRule(); // Default: 5 tx / 1 minute
    }

    private Transaction createTransaction(String accountId) {
        Transaction tx = new Transaction();
        tx.setTransactionId("550e8400-e29b-41d4-a716-446655440000");
        tx.setTimestamp("2026-02-16T14:30:00.000Z");
        tx.setAccountId(accountId);
        tx.setAmount(new BigDecimal("100.00"));
        tx.setCurrency("USD");
        return tx;
    }

    @Test
    @DisplayName("Count exceeding max triggers alert")
    void test_evaluateCount_exceedsMax_triggersAlert() {
        Transaction tx = createTransaction("ACC123456789");
        Optional<Alert> result = rule.evaluateCount(tx, 6);

        assertThat(result).isPresent();
        assertThat(result.get().getRuleName()).isEqualTo("rapid-activity");
        assertThat(result.get().getSeverity()).isEqualTo("medium");
    }

    @Test
    @DisplayName("Count at exactly max does not trigger alert")
    void test_evaluateCount_exactlyAtMax_noAlert() {
        Transaction tx = createTransaction("ACC123456789");
        Optional<Alert> result = rule.evaluateCount(tx, 5);

        assertThat(result).isEmpty();
    }

    @Test
    @DisplayName("Count below max does not trigger alert")
    void test_evaluateCount_belowMax_noAlert() {
        Transaction tx = createTransaction("ACC123456789");
        Optional<Alert> result = rule.evaluateCount(tx, 3);

        assertThat(result).isEmpty();
    }

    @Test
    @DisplayName("Alert description includes account ID and count")
    void test_evaluateCount_alertDescription_includesDetails() {
        Transaction tx = createTransaction("ACC123456789");
        Optional<Alert> result = rule.evaluateCount(tx, 7);

        assertThat(result).isPresent();
        assertThat(result.get().getDescription()).contains("ACC123456789");
        assertThat(result.get().getDescription()).contains("7");
    }

    @Test
    @DisplayName("Disabled rule returns empty")
    void test_evaluateCount_disabledRule_returnsEmpty() {
        RapidActivityRule disabled = new RapidActivityRule(5, 1, "medium", false);
        Transaction tx = createTransaction("ACC123456789");
        Optional<Alert> result = disabled.evaluateCount(tx, 10);

        assertThat(result).isEmpty();
    }

    @Test
    @DisplayName("Custom max count and window are respected")
    void test_evaluateCount_customConfig_respected() {
        RapidActivityRule custom = new RapidActivityRule(3, 2, "high", true);
        Transaction tx = createTransaction("ACC123456789");

        // 3 should not trigger (max is 3, needs to exceed)
        assertThat(custom.evaluateCount(tx, 3)).isEmpty();
        // 4 should trigger
        assertThat(custom.evaluateCount(tx, 4)).isPresent();
        assertThat(custom.evaluateCount(tx, 4).get().getSeverity()).isEqualTo("high");
    }

    @Test
    @DisplayName("Stateless evaluate always returns empty (needs state)")
    void test_evaluate_stateless_alwaysEmpty() {
        Transaction tx = createTransaction("ACC123456789");
        Optional<Alert> result = rule.evaluate(tx);

        assertThat(result).isEmpty();
    }

    @Test
    @DisplayName("Rule name is rapid-activity")
    void test_getRuleName_returnsCorrectName() {
        assertThat(rule.getRuleName()).isEqualTo("rapid-activity");
    }

    @Test
    @DisplayName("Default config: 5 max count, 1 minute window")
    void test_defaultConfig_correctValues() {
        assertThat(rule.getMaxCount()).isEqualTo(5);
        assertThat(rule.getWindowMinutes()).isEqualTo(1);
    }
}
