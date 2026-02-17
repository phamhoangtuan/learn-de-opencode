package com.learnde.pipeline.rules;

import com.learnde.pipeline.models.Alert;
import com.learnde.pipeline.models.Transaction;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalTime;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * T043: Unit tests for UnusualHourRule — time-based detection of transactions
 * occurring during configurable quiet hours.
 */
class UnusualHourRuleTest {

    private UnusualHourRule rule;

    @BeforeEach
    void setUp() {
        rule = new UnusualHourRule(); // Default: 01:00-05:00 UTC
    }

    private Transaction createTransactionAtTime(String isoTimestamp) {
        Transaction tx = new Transaction();
        tx.setTransactionId("550e8400-e29b-41d4-a716-446655440000");
        tx.setTimestamp(isoTimestamp);
        tx.setAccountId("ACC123456789");
        tx.setAmount(new BigDecimal("100.00"));
        tx.setCurrency("USD");
        return tx;
    }

    @Test
    @DisplayName("Transaction during quiet hours triggers alert")
    void test_evaluate_duringQuietHours_triggersAlert() {
        Transaction tx = createTransactionAtTime("2026-02-16T02:30:00.000Z");
        Optional<Alert> result = rule.evaluate(tx);

        assertThat(result).isPresent();
        assertThat(result.get().getRuleName()).isEqualTo("unusual-hour");
        assertThat(result.get().getSeverity()).isEqualTo("low");
    }

    @Test
    @DisplayName("Transaction at start of quiet hours triggers alert")
    void test_evaluate_atQuietStart_triggersAlert() {
        Transaction tx = createTransactionAtTime("2026-02-16T01:00:00.000Z");
        Optional<Alert> result = rule.evaluate(tx);

        assertThat(result).isPresent();
    }

    @Test
    @DisplayName("Transaction at end of quiet hours does NOT trigger alert")
    void test_evaluate_atQuietEnd_noAlert() {
        Transaction tx = createTransactionAtTime("2026-02-16T05:00:00.000Z");
        Optional<Alert> result = rule.evaluate(tx);

        assertThat(result).isEmpty();
    }

    @Test
    @DisplayName("Transaction during normal hours does not trigger alert")
    void test_evaluate_duringNormalHours_noAlert() {
        Transaction tx = createTransactionAtTime("2026-02-16T14:30:00.000Z");
        Optional<Alert> result = rule.evaluate(tx);

        assertThat(result).isEmpty();
    }

    @Test
    @DisplayName("Transaction at midnight (00:00) does not trigger (before quiet start)")
    void test_evaluate_atMidnight_noAlert() {
        Transaction tx = createTransactionAtTime("2026-02-16T00:00:00.000Z");
        Optional<Alert> result = rule.evaluate(tx);

        assertThat(result).isEmpty();
    }

    @Test
    @DisplayName("Custom quiet hours are respected")
    void test_evaluate_customQuietHours_respected() {
        UnusualHourRule custom = new UnusualHourRule(
                LocalTime.of(22, 0), LocalTime.of(6, 0), "medium", true);

        // 23:00 should trigger (within 22:00-06:00)
        Transaction txLate = createTransactionAtTime("2026-02-16T23:00:00.000Z");
        assertThat(custom.evaluate(txLate)).isPresent();

        // 03:00 should trigger (within 22:00-06:00)
        Transaction txEarly = createTransactionAtTime("2026-02-16T03:00:00.000Z");
        assertThat(custom.evaluate(txEarly)).isPresent();

        // 10:00 should NOT trigger
        Transaction txNormal = createTransactionAtTime("2026-02-16T10:00:00.000Z");
        assertThat(custom.evaluate(txNormal)).isEmpty();
    }

    @Test
    @DisplayName("Disabled rule returns empty")
    void test_evaluate_disabledRule_returnsEmpty() {
        UnusualHourRule disabled = new UnusualHourRule(
                LocalTime.of(1, 0), LocalTime.of(5, 0), "low", false);
        Transaction tx = createTransactionAtTime("2026-02-16T02:30:00.000Z");
        Optional<Alert> result = disabled.evaluate(tx);

        assertThat(result).isEmpty();
    }

    @Test
    @DisplayName("Null timestamp returns empty")
    void test_evaluate_nullTimestamp_returnsEmpty() {
        Transaction tx = createTransactionAtTime(null);
        tx.setTimestamp(null);
        Optional<Alert> result = rule.evaluate(tx);

        assertThat(result).isEmpty();
    }

    @Test
    @DisplayName("Invalid timestamp format returns empty")
    void test_evaluate_invalidTimestamp_returnsEmpty() {
        Transaction tx = createTransactionAtTime("not-a-date");
        Optional<Alert> result = rule.evaluate(tx);

        assertThat(result).isEmpty();
    }

    @Test
    @DisplayName("Alert description includes time and quiet period")
    void test_evaluate_alertDescription_includesDetails() {
        Transaction tx = createTransactionAtTime("2026-02-16T02:30:00.000Z");
        Optional<Alert> result = rule.evaluate(tx);

        assertThat(result).isPresent();
        assertThat(result.get().getDescription()).contains("02:30");
        assertThat(result.get().getDescription()).contains("01:00");
        assertThat(result.get().getDescription()).contains("05:00");
    }

    @Test
    @DisplayName("Rule name is unusual-hour")
    void test_getRuleName_returnsCorrectName() {
        assertThat(rule.getRuleName()).isEqualTo("unusual-hour");
    }

    @Test
    @DisplayName("Timezone conversion works correctly")
    void test_evaluate_timezoneConversion_worksCorrectly() {
        // 02:30 UTC+5 = 21:30 UTC (not in quiet hours)
        Transaction tx = createTransactionAtTime("2026-02-16T02:30:00.000+05:00");
        Optional<Alert> result = rule.evaluate(tx);

        // Should NOT trigger because 21:30 UTC is not in 01:00-05:00 UTC
        assertThat(result).isEmpty();
    }
}
