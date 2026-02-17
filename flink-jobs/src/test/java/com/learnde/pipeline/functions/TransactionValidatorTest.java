package com.learnde.pipeline.functions;

import com.learnde.pipeline.models.DeadLetterRecord;
import com.learnde.pipeline.models.Transaction;

import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * T039: Unit tests for TransactionValidator — schema validation of incoming transactions.
 *
 * Tests valid transactions pass through, and various invalid fields route to DLQ.
 */
class TransactionValidatorTest {

    private OneInputStreamOperatorTestHarness<Transaction, Transaction> testHarness;

    @BeforeEach
    void setUp() throws Exception {
        TransactionValidator validator = new TransactionValidator("test-validator-0");
        testHarness = new OneInputStreamOperatorTestHarness<>(
                new ProcessOperator<>(validator));
        testHarness.open();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (testHarness != null) {
            testHarness.close();
        }
    }

    private Transaction createValidTransaction() {
        Transaction tx = new Transaction();
        tx.setTransactionId("550e8400-e29b-41d4-a716-446655440000");
        tx.setTimestamp("2026-02-16T14:30:00.000Z");
        tx.setAccountId("ACC123456789");
        tx.setAmount(new BigDecimal("125.50"));
        tx.setCurrency("USD");
        tx.setMerchantName("Amazon");
        tx.setMerchantCategory("online");
        tx.setTransactionType("purchase");
        tx.setLocationCountry("US");
        tx.setStatus("completed");
        return tx;
    }

    private List<DeadLetterRecord> getDlqRecords() {
        var queue = testHarness.getSideOutput(TransactionValidator.DLQ_TAG);
        if (queue == null) return List.of();
        List<DeadLetterRecord> result = new ArrayList<>();
        for (StreamRecord<DeadLetterRecord> record : queue) {
            result.add(record.getValue());
        }
        return result;
    }

    @Test
    @DisplayName("Valid transaction passes validation and is emitted to main output")
    void test_processElement_validTransaction_emittedToMainOutput() throws Exception {
        Transaction tx = createValidTransaction();
        testHarness.processElement(tx, 1L);

        assertThat(testHarness.extractOutputValues()).hasSize(1);
        Transaction result = testHarness.extractOutputValues().get(0);
        assertThat(result.getTransactionId()).isEqualTo("550e8400-e29b-41d4-a716-446655440000");
        assertThat(result.getProcessingTimestamp()).isNotNull();
        assertThat(result.getPartitionDate()).isNotNull();
    }

    @Test
    @DisplayName("Valid transaction has enrichment fields set")
    void test_processElement_validTransaction_enrichmentFieldsSet() throws Exception {
        Transaction tx = createValidTransaction();
        testHarness.processElement(tx, 1L);

        Transaction result = testHarness.extractOutputValues().get(0);
        assertThat(result.getProcessingTimestamp()).isNotEmpty();
        assertThat(result.getPartitionDate()).isEqualTo("2026-02-16");
    }

    @Test
    @DisplayName("Missing transaction_id routes to DLQ")
    void test_processElement_missingTransactionId_routedToDlq() throws Exception {
        Transaction tx = createValidTransaction();
        tx.setTransactionId(null);
        testHarness.processElement(tx, 1L);

        assertThat(testHarness.extractOutputValues()).isEmpty();
        List<DeadLetterRecord> dlq = getDlqRecords();
        assertThat(dlq).hasSize(1);
        assertThat(dlq.get(0).getErrorField()).isEqualTo("transaction_id");
        assertThat(dlq.get(0).getErrorType()).isEqualTo("missing");
    }

    @Test
    @DisplayName("Invalid UUID format routes to DLQ")
    void test_processElement_invalidUuid_routedToDlq() throws Exception {
        Transaction tx = createValidTransaction();
        tx.setTransactionId("not-a-uuid");
        testHarness.processElement(tx, 1L);

        assertThat(testHarness.extractOutputValues()).isEmpty();
        List<DeadLetterRecord> dlq = getDlqRecords();
        assertThat(dlq).hasSize(1);
        assertThat(dlq.get(0).getErrorField()).isEqualTo("transaction_id");
        assertThat(dlq.get(0).getErrorType()).isEqualTo("invalid_format");
    }

    @Test
    @DisplayName("Invalid timestamp format routes to DLQ")
    void test_processElement_invalidTimestamp_routedToDlq() throws Exception {
        Transaction tx = createValidTransaction();
        tx.setTimestamp("not-a-date");
        testHarness.processElement(tx, 1L);

        assertThat(testHarness.extractOutputValues()).isEmpty();
        List<DeadLetterRecord> dlq = getDlqRecords();
        assertThat(dlq).hasSize(1);
        assertThat(dlq.get(0).getErrorField()).isEqualTo("timestamp");
        assertThat(dlq.get(0).getErrorType()).isEqualTo("invalid_format");
    }

    @Test
    @DisplayName("Invalid account_id pattern routes to DLQ")
    void test_processElement_invalidAccountId_routedToDlq() throws Exception {
        Transaction tx = createValidTransaction();
        tx.setAccountId("short");
        testHarness.processElement(tx, 1L);

        assertThat(testHarness.extractOutputValues()).isEmpty();
        List<DeadLetterRecord> dlq = getDlqRecords();
        assertThat(dlq).hasSize(1);
        assertThat(dlq.get(0).getErrorField()).isEqualTo("account_id");
        assertThat(dlq.get(0).getErrorType()).isEqualTo("invalid_format");
    }

    @Test
    @DisplayName("Zero amount routes to DLQ")
    void test_processElement_zeroAmount_routedToDlq() throws Exception {
        Transaction tx = createValidTransaction();
        tx.setAmount(BigDecimal.ZERO);
        testHarness.processElement(tx, 1L);

        assertThat(testHarness.extractOutputValues()).isEmpty();
        List<DeadLetterRecord> dlq = getDlqRecords();
        assertThat(dlq).hasSize(1);
        assertThat(dlq.get(0).getErrorField()).isEqualTo("amount");
        assertThat(dlq.get(0).getErrorType()).isEqualTo("out_of_range");
    }

    @Test
    @DisplayName("Amount exceeding max routes to DLQ")
    void test_processElement_amountExceedsMax_routedToDlq() throws Exception {
        Transaction tx = createValidTransaction();
        tx.setAmount(new BigDecimal("50001"));
        testHarness.processElement(tx, 1L);

        assertThat(testHarness.extractOutputValues()).isEmpty();
        List<DeadLetterRecord> dlq = getDlqRecords();
        assertThat(dlq).hasSize(1);
        assertThat(dlq.get(0).getErrorField()).isEqualTo("amount");
    }

    @Test
    @DisplayName("Invalid currency routes to DLQ")
    void test_processElement_invalidCurrency_routedToDlq() throws Exception {
        Transaction tx = createValidTransaction();
        tx.setCurrency("BTC");
        testHarness.processElement(tx, 1L);

        assertThat(testHarness.extractOutputValues()).isEmpty();
        List<DeadLetterRecord> dlq = getDlqRecords();
        assertThat(dlq).hasSize(1);
        assertThat(dlq.get(0).getErrorField()).isEqualTo("currency");
        assertThat(dlq.get(0).getErrorType()).isEqualTo("invalid_enum");
    }

    @Test
    @DisplayName("Invalid merchant_category routes to DLQ")
    void test_processElement_invalidMerchantCategory_routedToDlq() throws Exception {
        Transaction tx = createValidTransaction();
        tx.setMerchantCategory("gambling");
        testHarness.processElement(tx, 1L);

        assertThat(testHarness.extractOutputValues()).isEmpty();
        List<DeadLetterRecord> dlq = getDlqRecords();
        assertThat(dlq).hasSize(1);
    }

    @Test
    @DisplayName("Invalid transaction_type routes to DLQ")
    void test_processElement_invalidTransactionType_routedToDlq() throws Exception {
        Transaction tx = createValidTransaction();
        tx.setTransactionType("deposit");
        testHarness.processElement(tx, 1L);

        assertThat(testHarness.extractOutputValues()).isEmpty();
    }

    @Test
    @DisplayName("Invalid location_country routes to DLQ")
    void test_processElement_invalidCountry_routedToDlq() throws Exception {
        Transaction tx = createValidTransaction();
        tx.setLocationCountry("USA");
        testHarness.processElement(tx, 1L);

        assertThat(testHarness.extractOutputValues()).isEmpty();
        List<DeadLetterRecord> dlq = getDlqRecords();
        assertThat(dlq).hasSize(1);
        assertThat(dlq.get(0).getErrorField()).isEqualTo("location_country");
    }

    @Test
    @DisplayName("Invalid status routes to DLQ")
    void test_processElement_invalidStatus_routedToDlq() throws Exception {
        Transaction tx = createValidTransaction();
        tx.setStatus("processing");
        testHarness.processElement(tx, 1L);

        assertThat(testHarness.extractOutputValues()).isEmpty();
    }

    @Test
    @DisplayName("Negative amount (refund) passes validation")
    void test_processElement_negativeAmount_passesValidation() throws Exception {
        Transaction tx = createValidTransaction();
        tx.setAmount(new BigDecimal("-29.99"));
        tx.setTransactionType("refund");
        testHarness.processElement(tx, 1L);

        assertThat(testHarness.extractOutputValues()).hasSize(1);
    }

    @Test
    @DisplayName("DLQ record contains processor_id")
    void test_processElement_dlqRecord_containsProcessorId() throws Exception {
        Transaction tx = createValidTransaction();
        tx.setCurrency("XYZ");
        testHarness.processElement(tx, 1L);

        List<DeadLetterRecord> dlq = getDlqRecords();
        assertThat(dlq).hasSize(1);
        assertThat(dlq.get(0).getProcessorId()).isEqualTo("test-validator-0");
    }
}
