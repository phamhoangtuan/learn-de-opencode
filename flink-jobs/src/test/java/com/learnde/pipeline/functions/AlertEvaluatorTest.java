package com.learnde.pipeline.functions;

import com.learnde.pipeline.models.Alert;
import com.learnde.pipeline.models.Transaction;
import com.learnde.pipeline.rules.AlertingRule;
import com.learnde.pipeline.rules.HighValueRule;
import com.learnde.pipeline.rules.RapidActivityRule;
import com.learnde.pipeline.rules.UnusualHourRule;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * T040: Unit tests for AlertEvaluator — KeyedProcessFunction that evaluates
 * transactions against all enabled alerting rules.
 *
 * MUST include test case for multi-rule triggering: a single transaction matching
 * both HighValueRule and UnusualHourRule emits two independent alerts each
 * referencing the same transaction_id (spec.md edge case line 105).
 */
class AlertEvaluatorTest {

    private KeyedOneInputStreamOperatorTestHarness<String, Transaction, Alert> testHarness;

    @BeforeEach
    void setUp() throws Exception {
        List<AlertingRule> statelessRules = List.of(
                new HighValueRule(),
                new UnusualHourRule());
        RapidActivityRule rapidRule = new RapidActivityRule();

        AlertEvaluator evaluator = new AlertEvaluator(statelessRules, rapidRule);
        testHarness = new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(evaluator),
                Transaction::getAccountId,
                TypeInformation.of(String.class));
        testHarness.open();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (testHarness != null) {
            testHarness.close();
        }
    }

    private Transaction createTransaction(String accountId, BigDecimal amount, String timestamp) {
        Transaction tx = new Transaction();
        tx.setTransactionId(java.util.UUID.randomUUID().toString());
        tx.setTimestamp(timestamp);
        tx.setAccountId(accountId);
        tx.setAmount(amount);
        tx.setCurrency("USD");
        tx.setMerchantName("TestMerchant");
        tx.setMerchantCategory("online");
        tx.setTransactionType("purchase");
        tx.setLocationCountry("US");
        tx.setStatus("completed");
        return tx;
    }

    @Test
    @DisplayName("Normal transaction generates no alerts")
    void test_processElement_normalTransaction_noAlerts() throws Exception {
        Transaction tx = createTransaction("ACC123456789",
                new BigDecimal("100.00"), "2026-02-16T14:30:00.000Z");
        long eventTime = ZonedDateTime.parse(tx.getTimestamp()).toInstant().toEpochMilli();
        testHarness.processElement(tx, eventTime);

        assertThat(testHarness.extractOutputValues()).isEmpty();
    }

    @Test
    @DisplayName("High-value transaction generates alert")
    void test_processElement_highValue_generatesAlert() throws Exception {
        Transaction tx = createTransaction("ACC123456789",
                new BigDecimal("15000.00"), "2026-02-16T14:30:00.000Z");
        long eventTime = ZonedDateTime.parse(tx.getTimestamp()).toInstant().toEpochMilli();
        testHarness.processElement(tx, eventTime);

        List<Alert> alerts = testHarness.extractOutputValues();
        assertThat(alerts).hasSize(1);
        assertThat(alerts.get(0).getRuleName()).isEqualTo("high-value-transaction");
        assertThat(alerts.get(0).getTransactionId()).isEqualTo(tx.getTransactionId());
    }

    @Test
    @DisplayName("Unusual-hour transaction generates alert")
    void test_processElement_unusualHour_generatesAlert() throws Exception {
        Transaction tx = createTransaction("ACC123456789",
                new BigDecimal("100.00"), "2026-02-16T02:30:00.000Z");
        long eventTime = ZonedDateTime.parse(tx.getTimestamp()).toInstant().toEpochMilli();
        testHarness.processElement(tx, eventTime);

        List<Alert> alerts = testHarness.extractOutputValues();
        assertThat(alerts).hasSize(1);
        assertThat(alerts.get(0).getRuleName()).isEqualTo("unusual-hour");
    }

    @Test
    @DisplayName("Multi-rule triggering: high-value + unusual-hour emits two alerts with same transaction_id")
    void test_processElement_multiRule_emitsTwoAlerts() throws Exception {
        // Transaction that matches BOTH: high value AND unusual hour
        Transaction tx = createTransaction("ACC123456789",
                new BigDecimal("15000.00"), "2026-02-16T02:30:00.000Z");
        long eventTime = ZonedDateTime.parse(tx.getTimestamp()).toInstant().toEpochMilli();
        testHarness.processElement(tx, eventTime);

        List<Alert> alerts = testHarness.extractOutputValues();
        assertThat(alerts).hasSize(2);

        // Both alerts reference the same transaction_id
        assertThat(alerts.get(0).getTransactionId()).isEqualTo(tx.getTransactionId());
        assertThat(alerts.get(1).getTransactionId()).isEqualTo(tx.getTransactionId());

        // Each alert has a unique alert_id
        assertThat(alerts.get(0).getAlertId()).isNotEqualTo(alerts.get(1).getAlertId());

        // Both rule types present
        List<String> ruleNames = alerts.stream().map(Alert::getRuleName).sorted().toList();
        assertThat(ruleNames).containsExactly("high-value-transaction", "unusual-hour");
    }

    @Test
    @DisplayName("Transaction is_flagged is set to true when alert triggers")
    void test_processElement_alertTrigger_setsFlagged() throws Exception {
        Transaction tx = createTransaction("ACC123456789",
                new BigDecimal("15000.00"), "2026-02-16T14:30:00.000Z");
        long eventTime = ZonedDateTime.parse(tx.getTimestamp()).toInstant().toEpochMilli();
        testHarness.processElement(tx, eventTime);

        // Check enriched transaction side output
        var enrichedOutput = testHarness.getSideOutput(AlertEvaluator.ENRICHED_TX_TAG);
        assertThat(enrichedOutput).isNotNull();
        List<Transaction> enrichedTxs = new ArrayList<>();
        for (StreamRecord<Transaction> record : enrichedOutput) {
            enrichedTxs.add(record.getValue());
        }
        assertThat(enrichedTxs).hasSize(1);
        assertThat(enrichedTxs.get(0).isFlagged()).isTrue();
    }

    @Test
    @DisplayName("Normal transaction is_flagged remains false")
    void test_processElement_noAlert_flaggedFalse() throws Exception {
        Transaction tx = createTransaction("ACC123456789",
                new BigDecimal("100.00"), "2026-02-16T14:30:00.000Z");
        long eventTime = ZonedDateTime.parse(tx.getTimestamp()).toInstant().toEpochMilli();
        testHarness.processElement(tx, eventTime);

        var enrichedOutput = testHarness.getSideOutput(AlertEvaluator.ENRICHED_TX_TAG);
        List<Transaction> enrichedTxs = new ArrayList<>();
        for (StreamRecord<Transaction> record : enrichedOutput) {
            enrichedTxs.add(record.getValue());
        }
        assertThat(enrichedTxs).hasSize(1);
        assertThat(enrichedTxs.get(0).isFlagged()).isFalse();
    }

    @Test
    @DisplayName("Rapid activity: 6 transactions within 1 minute triggers alert")
    void test_processElement_rapidActivity_triggersAlert() throws Exception {
        String accountId = "ACC123456789";
        long baseTime = ZonedDateTime.parse("2026-02-16T14:30:00.000Z").toInstant().toEpochMilli();

        // Send 6 transactions within 1 minute (exceeds max of 5)
        for (int i = 0; i < 6; i++) {
            long eventTime = baseTime + (i * 10000L); // 10 seconds apart
            String ts = Instant.ofEpochMilli(eventTime).atZone(ZoneOffset.UTC).toString();
            Transaction tx = createTransaction(accountId, new BigDecimal("100.00"), ts);
            testHarness.processElement(tx, eventTime);
        }

        List<Alert> alerts = testHarness.extractOutputValues();
        // The 6th transaction should trigger the rapid-activity alert
        boolean hasRapidAlert = alerts.stream()
                .anyMatch(a -> "rapid-activity".equals(a.getRuleName()));
        assertThat(hasRapidAlert).isTrue();
    }

    @Test
    @DisplayName("4 transactions within 1 minute does NOT trigger rapid-activity")
    void test_processElement_belowRapidThreshold_noAlert() throws Exception {
        String accountId = "ACC987654321";
        long baseTime = ZonedDateTime.parse("2026-02-16T14:30:00.000Z").toInstant().toEpochMilli();

        for (int i = 0; i < 4; i++) {
            long eventTime = baseTime + (i * 10000L);
            String ts = Instant.ofEpochMilli(eventTime).atZone(ZoneOffset.UTC).toString();
            Transaction tx = createTransaction(accountId, new BigDecimal("100.00"), ts);
            testHarness.processElement(tx, eventTime);
        }

        List<Alert> alerts = testHarness.extractOutputValues();
        boolean hasRapidAlert = alerts.stream()
                .anyMatch(a -> "rapid-activity".equals(a.getRuleName()));
        assertThat(hasRapidAlert).isFalse();
    }

    @Test
    @DisplayName("Rapid activity for different accounts counted separately")
    void test_processElement_differentAccounts_countedSeparately() throws Exception {
        long baseTime = ZonedDateTime.parse("2026-02-16T14:30:00.000Z").toInstant().toEpochMilli();

        // 3 transactions for account A
        for (int i = 0; i < 3; i++) {
            long eventTime = baseTime + (i * 10000L);
            String ts = Instant.ofEpochMilli(eventTime).atZone(ZoneOffset.UTC).toString();
            Transaction tx = createTransaction("ACCT000000A0", new BigDecimal("100.00"), ts);
            testHarness.processElement(tx, eventTime);
        }

        // 3 transactions for account B
        for (int i = 0; i < 3; i++) {
            long eventTime = baseTime + (i * 10000L);
            String ts = Instant.ofEpochMilli(eventTime).atZone(ZoneOffset.UTC).toString();
            Transaction tx = createTransaction("ACCT000000B0", new BigDecimal("100.00"), ts);
            testHarness.processElement(tx, eventTime);
        }

        // Neither should trigger (3 < 5 threshold)
        List<Alert> alerts = testHarness.extractOutputValues();
        boolean hasRapidAlert = alerts.stream()
                .anyMatch(a -> "rapid-activity".equals(a.getRuleName()));
        assertThat(hasRapidAlert).isFalse();
    }

    @Test
    @DisplayName("Event-time semantics: out-of-order arrivals trigger based on event-time window")
    void test_processElement_outOfOrderArrivals_eventTimeWindow() throws Exception {
        String accountId = "ACC123456789";
        long baseTime = ZonedDateTime.parse("2026-02-16T14:30:00.000Z").toInstant().toEpochMilli();

        // Send 6 transactions with event timestamps within 1 minute,
        // but arriving out of order (shuffled processing-time)
        long[] eventTimes = {
                baseTime,               // t=0s
                baseTime + 40000L,      // t=40s  (arrives 2nd)
                baseTime + 10000L,      // t=10s  (arrives 3rd)
                baseTime + 50000L,      // t=50s  (arrives 4th)
                baseTime + 20000L,      // t=20s  (arrives 5th)
                baseTime + 30000L       // t=30s  (arrives 6th)
        };

        for (long eventTime : eventTimes) {
            String ts = Instant.ofEpochMilli(eventTime).atZone(ZoneOffset.UTC).toString();
            Transaction tx = createTransaction(accountId, new BigDecimal("100.00"), ts);
            testHarness.processElement(tx, eventTime);
        }

        List<Alert> alerts = testHarness.extractOutputValues();
        boolean hasRapidAlert = alerts.stream()
                .anyMatch(a -> "rapid-activity".equals(a.getRuleName()));
        assertThat(hasRapidAlert)
                .as("Should trigger rapid-activity based on event-time window despite out-of-order arrivals")
                .isTrue();
    }
}
