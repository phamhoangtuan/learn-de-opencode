package com.learnde.pipeline.functions;

import com.learnde.pipeline.models.Alert;
import com.learnde.pipeline.models.Transaction;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZonedDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for IcebergSinkBuilder.
 *
 * <p>Verifies configuration validation, builder parameter handling,
 * and POJO-to-RowData conversion for Iceberg sinks.
 * Integration with a live Iceberg catalog is tested in T044.
 */
@DisplayName("IcebergSinkBuilder")
class IcebergSinkBuilderTest {

    @Nested
    @DisplayName("Configuration validation")
    class ConfigValidation {

        @Test
        @DisplayName("default catalog URI is http://iceberg-rest:8181")
        void defaultCatalogUri_isIcebergRest() {
            IcebergSinkBuilder builder = new IcebergSinkBuilder();
            assertThat(builder.getCatalogUri()).isEqualTo("http://iceberg-rest:8181");
        }

        @Test
        @DisplayName("default warehouse is /warehouse")
        void defaultWarehouse_isWarehouse() {
            IcebergSinkBuilder builder = new IcebergSinkBuilder();
            assertThat(builder.getWarehouse()).isEqualTo("/warehouse");
        }

        @Test
        @DisplayName("custom catalog URI is accepted")
        void customCatalogUri_isUsed() {
            IcebergSinkBuilder builder = new IcebergSinkBuilder("http://localhost:8181", "/custom-wh");
            assertThat(builder.getCatalogUri()).isEqualTo("http://localhost:8181");
        }

        @Test
        @DisplayName("custom warehouse is accepted")
        void customWarehouse_isUsed() {
            IcebergSinkBuilder builder = new IcebergSinkBuilder("http://localhost:8181", "/custom-wh");
            assertThat(builder.getWarehouse()).isEqualTo("/custom-wh");
        }

        @Test
        @DisplayName("null catalog URI throws IllegalArgumentException")
        void nullCatalogUri_throwsException() {
            assertThatThrownBy(() -> new IcebergSinkBuilder(null, "/warehouse"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("catalogUri");
        }

        @Test
        @DisplayName("empty catalog URI throws IllegalArgumentException")
        void emptyCatalogUri_throwsException() {
            assertThatThrownBy(() -> new IcebergSinkBuilder("", "/warehouse"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("catalogUri");
        }

        @Test
        @DisplayName("null warehouse throws IllegalArgumentException")
        void nullWarehouse_throwsException() {
            assertThatThrownBy(() -> new IcebergSinkBuilder("http://localhost:8181", null))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("warehouse");
        }

        @Test
        @DisplayName("blank warehouse throws IllegalArgumentException")
        void blankWarehouse_throwsException() {
            assertThatThrownBy(() -> new IcebergSinkBuilder("http://localhost:8181", "  "))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("warehouse");
        }
    }

    @Nested
    @DisplayName("Table names")
    class TableNames {

        @Test
        @DisplayName("transactions table identifier is financial.transactions")
        void transactionsTableIdentifier() {
            assertThat(IcebergSinkBuilder.TRANSACTIONS_TABLE)
                    .isEqualTo("financial.transactions");
        }

        @Test
        @DisplayName("alerts table identifier is financial.alerts")
        void alertsTableIdentifier() {
            assertThat(IcebergSinkBuilder.ALERTS_TABLE)
                    .isEqualTo("financial.alerts");
        }
    }

    @Nested
    @DisplayName("Catalog properties")
    class CatalogProperties {

        @Test
        @DisplayName("catalog properties contain correct type")
        void catalogProperties_containType() {
            IcebergSinkBuilder builder = new IcebergSinkBuilder();
            var props = builder.getCatalogProperties();
            assertThat(props).containsEntry("type", "rest");
        }

        @Test
        @DisplayName("catalog properties contain correct URI")
        void catalogProperties_containUri() {
            IcebergSinkBuilder builder = new IcebergSinkBuilder("http://custom:8181", "/wh");
            var props = builder.getCatalogProperties();
            assertThat(props).containsEntry("uri", "http://custom:8181");
        }

        @Test
        @DisplayName("catalog properties contain correct warehouse")
        void catalogProperties_containWarehouse() {
            IcebergSinkBuilder builder = new IcebergSinkBuilder("http://custom:8181", "/my-wh");
            var props = builder.getCatalogProperties();
            assertThat(props).containsEntry("warehouse", "/my-wh");
        }

        @Test
        @DisplayName("catalog properties contain IO implementation")
        void catalogProperties_containIoImpl() {
            IcebergSinkBuilder builder = new IcebergSinkBuilder();
            var props = builder.getCatalogProperties();
            assertThat(props).containsEntry("io-impl", "org.apache.iceberg.io.ResolvingFileIO");
        }
    }

    @Nested
    @DisplayName("Serialization")
    class Serialization {

        @Test
        @DisplayName("IcebergSinkBuilder is serializable")
        void isSerializable() throws Exception {
            IcebergSinkBuilder builder = new IcebergSinkBuilder();
            java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
            java.io.ObjectOutputStream oos = new java.io.ObjectOutputStream(bos);
            oos.writeObject(builder);
            oos.close();

            java.io.ByteArrayInputStream bis = new java.io.ByteArrayInputStream(bos.toByteArray());
            java.io.ObjectInputStream ois = new java.io.ObjectInputStream(bis);
            IcebergSinkBuilder deserialized = (IcebergSinkBuilder) ois.readObject();

            assertThat(deserialized.getCatalogUri()).isEqualTo(builder.getCatalogUri());
            assertThat(deserialized.getWarehouse()).isEqualTo(builder.getWarehouse());
        }
    }

    @Nested
    @DisplayName("TransactionToRowData converter")
    class TransactionToRowDataTest {

        private final IcebergSinkBuilder.TransactionToRowData converter =
                new IcebergSinkBuilder.TransactionToRowData();

        private Transaction createTestTransaction() {
            Transaction tx = new Transaction();
            tx.setTransactionId("550e8400-e29b-41d4-a716-446655440000");
            tx.setTimestamp("2026-02-17T10:30:00Z");
            tx.setAccountId("ACC123456789");
            tx.setAmount(new BigDecimal("1234.56"));
            tx.setCurrency("USD");
            tx.setMerchantName("Test Merchant");
            tx.setMerchantCategory("retail");
            tx.setTransactionType("purchase");
            tx.setLocationCountry("US");
            tx.setStatus("completed");
            tx.setProcessingTimestamp("2026-02-17T10:30:01Z");
            tx.setPartitionDate("2026-02-17");
            tx.setFlagged(true);
            return tx;
        }

        @Test
        @DisplayName("converts transaction_id to StringData at index 0")
        void convertsTransactionId() throws Exception {
            RowData row = converter.map(createTestTransaction());
            assertThat(row.getString(0).toString())
                    .isEqualTo("550e8400-e29b-41d4-a716-446655440000");
        }

        @Test
        @DisplayName("converts timestamp to TimestampData at index 1")
        void convertsTimestamp() throws Exception {
            RowData row = converter.map(createTestTransaction());
            TimestampData ts = row.getTimestamp(1, 6);
            Instant expected = ZonedDateTime.parse("2026-02-17T10:30:00Z").toInstant();
            assertThat(ts.toInstant()).isEqualTo(expected);
        }

        @Test
        @DisplayName("converts account_id to StringData at index 2")
        void convertsAccountId() throws Exception {
            RowData row = converter.map(createTestTransaction());
            assertThat(row.getString(2).toString()).isEqualTo("ACC123456789");
        }

        @Test
        @DisplayName("converts amount to DecimalData at index 3")
        void convertsAmount() throws Exception {
            RowData row = converter.map(createTestTransaction());
            DecimalData dec = row.getDecimal(3, 10, 2);
            assertThat(dec.toBigDecimal()).isEqualByComparingTo(new BigDecimal("1234.56"));
        }

        @Test
        @DisplayName("converts currency to StringData at index 4")
        void convertsCurrency() throws Exception {
            RowData row = converter.map(createTestTransaction());
            assertThat(row.getString(4).toString()).isEqualTo("USD");
        }

        @Test
        @DisplayName("converts merchant_name to StringData at index 5")
        void convertsMerchantName() throws Exception {
            RowData row = converter.map(createTestTransaction());
            assertThat(row.getString(5).toString()).isEqualTo("Test Merchant");
        }

        @Test
        @DisplayName("converts is_flagged to boolean at index 12")
        void convertsFlagged() throws Exception {
            RowData row = converter.map(createTestTransaction());
            assertThat(row.getBoolean(12)).isTrue();
        }

        @Test
        @DisplayName("converts partition_date to int (epoch days) at index 11")
        void convertsPartitionDate() throws Exception {
            RowData row = converter.map(createTestTransaction());
            int epochDay = row.getInt(11);
            LocalDate expected = LocalDate.of(2026, 2, 17);
            assertThat(epochDay).isEqualTo((int) expected.toEpochDay());
        }

        @Test
        @DisplayName("handles null timestamp by using current time")
        void handlesNullTimestamp() throws Exception {
            Transaction tx = createTestTransaction();
            tx.setTimestamp(null);
            RowData row = converter.map(tx);
            // Should not throw, and should produce a non-null timestamp
            assertThat(row.getTimestamp(1, 6)).isNotNull();
        }

        @Test
        @DisplayName("handles null amount by using zero")
        void handlesNullAmount() throws Exception {
            Transaction tx = createTestTransaction();
            tx.setAmount(null);
            RowData row = converter.map(tx);
            assertThat(row.getDecimal(3, 10, 2).toBigDecimal())
                    .isEqualByComparingTo(BigDecimal.ZERO);
        }

        @Test
        @DisplayName("handles null processing_timestamp as null field")
        void handlesNullProcessingTimestamp() throws Exception {
            Transaction tx = createTestTransaction();
            tx.setProcessingTimestamp(null);
            RowData row = converter.map(tx);
            assertThat(row.isNullAt(10)).isTrue();
        }

        @Test
        @DisplayName("handles null partition_date as null field")
        void handlesNullPartitionDate() throws Exception {
            Transaction tx = createTestTransaction();
            tx.setPartitionDate(null);
            RowData row = converter.map(tx);
            assertThat(row.isNullAt(11)).isTrue();
        }

        @Test
        @DisplayName("row has 13 fields matching Iceberg schema")
        void rowHas13Fields() throws Exception {
            RowData row = converter.map(createTestTransaction());
            assertThat(row.getArity()).isEqualTo(13);
        }
    }

    @Nested
    @DisplayName("AlertToRowData converter")
    class AlertToRowDataTest {

        private final IcebergSinkBuilder.AlertToRowData converter =
                new IcebergSinkBuilder.AlertToRowData();

        private Alert createTestAlert() {
            return new Alert(
                    "tx-001",
                    "high-value-transaction",
                    "high",
                    "2026-02-17T10:30:01Z",
                    "Transaction amount exceeds threshold");
        }

        @Test
        @DisplayName("converts alert_id to StringData at index 0")
        void convertsAlertId() throws Exception {
            RowData row = converter.map(createTestAlert());
            assertThat(row.getString(0).toString()).isNotEmpty(); // UUID generated
        }

        @Test
        @DisplayName("converts transaction_id to StringData at index 1")
        void convertsTransactionId() throws Exception {
            RowData row = converter.map(createTestAlert());
            assertThat(row.getString(1).toString()).isEqualTo("tx-001");
        }

        @Test
        @DisplayName("converts rule_name to StringData at index 2")
        void convertsRuleName() throws Exception {
            RowData row = converter.map(createTestAlert());
            assertThat(row.getString(2).toString()).isEqualTo("high-value-transaction");
        }

        @Test
        @DisplayName("converts severity to StringData at index 3")
        void convertsSeverity() throws Exception {
            RowData row = converter.map(createTestAlert());
            assertThat(row.getString(3).toString()).isEqualTo("high");
        }

        @Test
        @DisplayName("converts alert_timestamp to TimestampData at index 4")
        void convertsAlertTimestamp() throws Exception {
            RowData row = converter.map(createTestAlert());
            TimestampData ts = row.getTimestamp(4, 6);
            assertThat(ts.toInstant()).isEqualTo(Instant.parse("2026-02-17T10:30:01Z"));
        }

        @Test
        @DisplayName("converts description to StringData at index 5")
        void convertsDescription() throws Exception {
            RowData row = converter.map(createTestAlert());
            assertThat(row.getString(5).toString())
                    .isEqualTo("Transaction amount exceeds threshold");
        }

        @Test
        @DisplayName("row has 6 fields matching Iceberg schema")
        void rowHas6Fields() throws Exception {
            RowData row = converter.map(createTestAlert());
            assertThat(row.getArity()).isEqualTo(6);
        }

        @Test
        @DisplayName("handles null alert_timestamp by using current time")
        void handlesNullAlertTimestamp() throws Exception {
            Alert alert = new Alert("tx-001", "test-rule", "low", null, "desc");
            RowData row = converter.map(alert);
            assertThat(row.getTimestamp(4, 6)).isNotNull();
        }
    }
}
