package com.learnde.pipeline.functions;

import com.learnde.pipeline.models.Alert;
import com.learnde.pipeline.models.Transaction;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.DecimalData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.catalog.TableIdentifier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * IcebergSinkBuilder — configures Iceberg table writers for the pipeline.
 *
 * <p>Creates FlinkSink instances for the transactions and alerts Iceberg tables
 * using the REST catalog. Configuration:
 * <ul>
 *   <li>Format: Parquet with ZSTD compression</li>
 *   <li>Target file size: 128MB</li>
 *   <li>Checkpoint interval configured at Flink level (5-10 min)</li>
 * </ul>
 *
 * <p>Tables are pre-created by the init-iceberg service (PyIceberg).
 * This builder connects to the existing catalog and appends data.
 */
public class IcebergSinkBuilder implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(IcebergSinkBuilder.class);

    /** Fully qualified Iceberg table name for transactions. */
    public static final String TRANSACTIONS_TABLE = "financial.transactions";
    /** Fully qualified Iceberg table name for alerts. */
    public static final String ALERTS_TABLE = "financial.alerts";

    private static final String CATALOG_NAME = "rest";

    private final String catalogUri;
    private final String warehouse;

    /**
     * Create an IcebergSinkBuilder with default catalog settings.
     * Defaults: catalog at http://iceberg-rest:8181, warehouse at /warehouse.
     */
    public IcebergSinkBuilder() {
        this("http://iceberg-rest:8181", "/warehouse");
    }

    /**
     * Create an IcebergSinkBuilder with custom catalog settings.
     *
     * @param catalogUri the Iceberg REST catalog URI
     * @param warehouse  the warehouse path
     * @throws IllegalArgumentException if catalogUri or warehouse is null/blank
     */
    public IcebergSinkBuilder(String catalogUri, String warehouse) {
        if (catalogUri == null || catalogUri.isBlank()) {
            throw new IllegalArgumentException("catalogUri must not be null or blank");
        }
        if (warehouse == null || warehouse.isBlank()) {
            throw new IllegalArgumentException("warehouse must not be null or blank");
        }
        this.catalogUri = catalogUri;
        this.warehouse = warehouse;
    }

    /**
     * Get the catalog URI.
     *
     * @return the Iceberg REST catalog URI
     */
    public String getCatalogUri() {
        return catalogUri;
    }

    /**
     * Get the warehouse path.
     *
     * @return the warehouse path
     */
    public String getWarehouse() {
        return warehouse;
    }

    /**
     * Get the catalog properties map for Iceberg REST catalog configuration.
     *
     * @return map of catalog properties
     */
    public Map<String, String> getCatalogProperties() {
        Map<String, String> props = new HashMap<>();
        props.put("type", "rest");
        props.put("uri", catalogUri);
        props.put("warehouse", warehouse);
        props.put("io-impl", "org.apache.iceberg.io.ResolvingFileIO");
        return props;
    }

    /**
     * Create a CatalogLoader for the Iceberg REST catalog.
     *
     * @return a CatalogLoader instance
     */
    CatalogLoader createCatalogLoader() {
        return CatalogLoader.rest(CATALOG_NAME, new Configuration(), getCatalogProperties());
    }

    /**
     * Create a TableLoader for the given Iceberg table identifier.
     *
     * @param tableIdentifier fully qualified table name (e.g., "financial.transactions")
     * @return a TableLoader instance
     */
    public TableLoader createTableLoader(String tableIdentifier) {
        LOG.info("Creating TableLoader for table '{}' at catalog '{}'", tableIdentifier, catalogUri);
        return TableLoader.fromCatalog(createCatalogLoader(),
                TableIdentifier.parse(tableIdentifier));
    }

    /**
     * Attach an Iceberg sink for transactions to the given DataStream.
     *
     * <p>Converts Transaction POJOs to RowData and appends to the
     * financial.transactions Iceberg table.
     *
     * @param transactionStream the transaction DataStream
     */
    public void buildTransactionSink(DataStream<Transaction> transactionStream) {
        LOG.info("Attaching Iceberg transaction sink for table '{}'", TRANSACTIONS_TABLE);

        TableLoader tableLoader = createTableLoader(TRANSACTIONS_TABLE);

        FlinkSink.builderFor(
                transactionStream,
                new TransactionToRowData(),
                TypeInformation.of(RowData.class))
                .tableLoader(tableLoader)
                .upsert(false)
                .append();

        LOG.info("Iceberg transaction sink attached for table '{}'", TRANSACTIONS_TABLE);
    }

    /**
     * Attach an Iceberg sink for alerts to the given DataStream.
     *
     * <p>Converts Alert POJOs to RowData and appends to the
     * financial.alerts Iceberg table.
     *
     * @param alertStream the alert DataStream
     */
    public void buildAlertSink(DataStream<Alert> alertStream) {
        LOG.info("Attaching Iceberg alert sink for table '{}'", ALERTS_TABLE);

        TableLoader tableLoader = createTableLoader(ALERTS_TABLE);

        FlinkSink.builderFor(
                alertStream,
                new AlertToRowData(),
                TypeInformation.of(RowData.class))
                .tableLoader(tableLoader)
                .upsert(false)
                .append();

        LOG.info("Iceberg alert sink attached for table '{}'", ALERTS_TABLE);
    }

    /**
     * MapFunction that converts Transaction POJOs to RowData for Iceberg.
     *
     * <p>Maps to the schema defined in init_iceberg.py (13 fields):
     * transaction_id, timestamp, account_id, amount, currency,
     * merchant_name, merchant_category, transaction_type, location_country,
     * status, processing_timestamp, partition_date, is_flagged.
     */
    static class TransactionToRowData implements MapFunction<Transaction, RowData> {

        private static final long serialVersionUID = 1L;

        @Override
        public RowData map(Transaction tx) throws Exception {
            GenericRowData row = new GenericRowData(13);

            row.setField(0, StringData.fromString(tx.getTransactionId()));

            // timestamp → TimestampData (microsecond precision with timezone)
            if (tx.getTimestamp() != null) {
                try {
                    Instant instant = ZonedDateTime.parse(tx.getTimestamp()).toInstant();
                    row.setField(1, TimestampData.fromInstant(instant));
                } catch (Exception e) {
                    row.setField(1, TimestampData.fromInstant(Instant.now()));
                }
            } else {
                row.setField(1, TimestampData.fromInstant(Instant.now()));
            }

            row.setField(2, StringData.fromString(tx.getAccountId()));

            // amount → DecimalData (precision=10, scale=2)
            BigDecimal amount = tx.getAmount() != null ? tx.getAmount() : BigDecimal.ZERO;
            row.setField(3, DecimalData.fromBigDecimal(amount, 10, 2));

            row.setField(4, StringData.fromString(tx.getCurrency()));
            row.setField(5, StringData.fromString(tx.getMerchantName()));
            row.setField(6, StringData.fromString(tx.getMerchantCategory()));
            row.setField(7, StringData.fromString(tx.getTransactionType()));
            row.setField(8, StringData.fromString(tx.getLocationCountry()));
            row.setField(9, StringData.fromString(tx.getStatus()));

            // processing_timestamp → TimestampData
            if (tx.getProcessingTimestamp() != null) {
                try {
                    Instant procInstant = Instant.parse(tx.getProcessingTimestamp());
                    row.setField(10, TimestampData.fromInstant(procInstant));
                } catch (Exception e) {
                    row.setField(10, TimestampData.fromInstant(Instant.now()));
                }
            } else {
                row.setField(10, null);
            }

            // partition_date → int (days since epoch)
            if (tx.getPartitionDate() != null) {
                try {
                    LocalDate date = LocalDate.parse(tx.getPartitionDate());
                    row.setField(11, (int) date.toEpochDay());
                } catch (Exception e) {
                    row.setField(11, null);
                }
            } else {
                row.setField(11, null);
            }

            // is_flagged → boolean
            row.setField(12, tx.isFlagged());

            return row;
        }
    }

    /**
     * MapFunction that converts Alert POJOs to RowData for Iceberg.
     *
     * <p>Maps to the schema defined in init_iceberg.py (6 fields):
     * alert_id, transaction_id, rule_name, severity, alert_timestamp, description.
     */
    static class AlertToRowData implements MapFunction<Alert, RowData> {

        private static final long serialVersionUID = 1L;

        @Override
        public RowData map(Alert alert) throws Exception {
            GenericRowData row = new GenericRowData(6);

            row.setField(0, StringData.fromString(alert.getAlertId()));
            row.setField(1, StringData.fromString(alert.getTransactionId()));
            row.setField(2, StringData.fromString(alert.getRuleName()));
            row.setField(3, StringData.fromString(alert.getSeverity()));

            // alert_timestamp → TimestampData
            if (alert.getAlertTimestamp() != null) {
                try {
                    Instant instant = Instant.parse(alert.getAlertTimestamp());
                    row.setField(4, TimestampData.fromInstant(instant));
                } catch (Exception e) {
                    row.setField(4, TimestampData.fromInstant(Instant.now()));
                }
            } else {
                row.setField(4, TimestampData.fromInstant(Instant.now()));
            }

            row.setField(5, StringData.fromString(alert.getDescription()));

            return row;
        }
    }
}
