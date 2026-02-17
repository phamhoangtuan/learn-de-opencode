package com.learnde.pipeline;

import com.learnde.pipeline.functions.AlertEvaluator;
import com.learnde.pipeline.functions.IcebergSinkBuilder;
import com.learnde.pipeline.functions.TransactionValidator;
import com.learnde.pipeline.models.Alert;
import com.learnde.pipeline.models.DeadLetterRecord;
import com.learnde.pipeline.models.Transaction;
import com.learnde.pipeline.rules.AlertingRule;
import com.learnde.pipeline.rules.RapidActivityRule;
import com.learnde.pipeline.rules.RuleConfigLoader;
import com.learnde.pipeline.serialization.AlertSerializer;
import com.learnde.pipeline.serialization.TransactionDeserializer;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.List;

/**
 * TransactionPipeline — main Flink job for the streaming financial pipeline.
 *
 * <p>Pipeline topology:
 * <pre>
 *   Kafka (raw-transactions)
 *       → Deserialize (TransactionDeserializer)
 *       → Filter nulls (deserialization failures)
 *       → Validate (TransactionValidator)
 *           ├── Valid → AlertEvaluator (keyed by account_id)
 *           │       ├── Alerts → Kafka (alerts topic) + Iceberg (alerts table)
 *           │       └── Enriched Transactions → Iceberg (transactions table)
 *           └── Invalid → DLQ side output → Kafka (dead-letter topic)
 * </pre>
 *
 * <p>Configurable via command-line arguments or environment defaults.
 * Alerting rules loaded from YAML config (mounted volume).
 */
public class TransactionPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionPipeline.class);

    public static void main(String[] args) throws Exception {
        LOG.info("Starting TransactionPipeline...");

        PipelineConfig config = PipelineConfig.fromArgs(args);
        LOG.info("Pipeline configuration: {}", config);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // --- Load alerting rules from YAML config ---
        RuleConfigLoader ruleLoader = new RuleConfigLoader(config.getRulesConfigPath());
        List<AlertingRule> statelessRules = ruleLoader.loadStatelessRules();
        RapidActivityRule rapidActivityRule = ruleLoader.loadRapidActivityRule();
        LOG.info("Loaded {} stateless rules, rapid-activity rule enabled={}",
                statelessRules.size(), rapidActivityRule.isEnabled());

        // --- Kafka Source: raw-transactions ---
        KafkaSource<Transaction> kafkaSource = KafkaSource.<Transaction>builder()
                .setBootstrapServers(config.getKafkaBootstrapServers())
                .setTopics(config.getInputTopic())
                .setGroupId(config.getConsumerGroup())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new TransactionDeserializer())
                .build();

        // Watermark strategy: extract event time from transaction timestamp
        WatermarkStrategy<Transaction> watermarkStrategy = WatermarkStrategy
                .<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((tx, recordTimestamp) -> {
                    if (tx != null && tx.getTimestamp() != null) {
                        try {
                            return ZonedDateTime.parse(tx.getTimestamp())
                                    .toInstant().toEpochMilli();
                        } catch (Exception e) {
                            return recordTimestamp;
                        }
                    }
                    return recordTimestamp;
                })
                .withIdleness(Duration.ofMinutes(1));

        DataStream<Transaction> rawTransactions = env
                .fromSource(kafkaSource, watermarkStrategy, "Kafka: raw-transactions")
                .filter(tx -> tx != null)
                .name("Filter: null records");

        // --- Validate ---
        SingleOutputStreamOperator<Transaction> validTransactions = rawTransactions
                .process(new TransactionValidator(config.getConsumerGroup()))
                .name("Validate: schema + business rules");

        // DLQ side output
        DataStream<DeadLetterRecord> dlqRecords = validTransactions
                .getSideOutput(TransactionValidator.DLQ_TAG);

        // --- Alert Evaluation (keyed by account_id) ---
        SingleOutputStreamOperator<Alert> alerts = validTransactions
                .keyBy(Transaction::getAccountId)
                .process(new AlertEvaluator(statelessRules, rapidActivityRule))
                .name("Evaluate: alerting rules");

        // Enriched transactions via side output
        DataStream<Transaction> enrichedTransactions = alerts
                .getSideOutput(AlertEvaluator.ENRICHED_TX_TAG);

        // --- Kafka Sink: alerts ---
        KafkaSink<Alert> alertKafkaSink = KafkaSink.<Alert>builder()
                .setBootstrapServers(config.getKafkaBootstrapServers())
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(config.getAlertTopic())
                        .setValueSerializationSchema(new AlertSerializer())
                        .build())
                .build();
        alerts.sinkTo(alertKafkaSink).name("Kafka Sink: alerts");

        // --- Kafka Sink: dead-letter ---
        KafkaSink<String> dlqKafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers(config.getKafkaBootstrapServers())
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(config.getDeadLetterTopic())
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();

        dlqRecords
                .map(dlr -> {
                    com.fasterxml.jackson.databind.ObjectMapper mapper =
                            new com.fasterxml.jackson.databind.ObjectMapper();
                    return mapper.writeValueAsString(dlr);
                })
                .returns(String.class)
                .name("Serialize: DLQ records")
                .sinkTo(dlqKafkaSink)
                .name("Kafka Sink: dead-letter");

        // --- Iceberg Sinks ---
        IcebergSinkBuilder icebergSinkBuilder = new IcebergSinkBuilder(
                config.getCatalogUri(), config.getWarehouse());

        icebergSinkBuilder.buildTransactionSink(enrichedTransactions);
        icebergSinkBuilder.buildAlertSink(alerts);

        LOG.info("Pipeline topology assembled, starting execution...");
        env.execute("Streaming Financial Transaction Pipeline");
    }

    /**
     * Pipeline configuration with defaults and argument parsing.
     */
    public static class PipelineConfig implements Serializable {

        private static final long serialVersionUID = 1L;

        private String kafkaBootstrapServers;
        private String inputTopic;
        private String alertTopic;
        private String deadLetterTopic;
        private String consumerGroup;
        private String rulesConfigPath;
        private String catalogUri;
        private String warehouse;

        private PipelineConfig() {
            // defaults
            this.kafkaBootstrapServers = "kafka:29092";
            this.inputTopic = "raw-transactions";
            this.alertTopic = "alerts";
            this.deadLetterTopic = "dead-letter";
            this.consumerGroup = "transaction-pipeline";
            this.rulesConfigPath = "/opt/flink/config/alerting-rules.yaml";
            this.catalogUri = "http://iceberg-rest:8181";
            this.warehouse = "/warehouse";
        }

        /**
         * Create a PipelineConfig with all defaults.
         *
         * @return a default PipelineConfig
         */
        public static PipelineConfig fromDefaults() {
            return new PipelineConfig();
        }

        /**
         * Create a PipelineConfig from command-line arguments.
         * Supports --key value pairs. Unknown keys are ignored.
         *
         * @param args command-line arguments
         * @return a PipelineConfig with overridden values
         */
        public static PipelineConfig fromArgs(String[] args) {
            PipelineConfig config = new PipelineConfig();
            for (int i = 0; i < args.length - 1; i += 2) {
                String key = args[i];
                String value = args[i + 1];
                switch (key) {
                    case "--kafka.bootstrap.servers":
                        config.kafkaBootstrapServers = value;
                        break;
                    case "--input.topic":
                        config.inputTopic = value;
                        break;
                    case "--alert.topic":
                        config.alertTopic = value;
                        break;
                    case "--dead-letter.topic":
                        config.deadLetterTopic = value;
                        break;
                    case "--consumer.group":
                        config.consumerGroup = value;
                        break;
                    case "--rules.config.path":
                        config.rulesConfigPath = value;
                        break;
                    case "--catalog.uri":
                        config.catalogUri = value;
                        break;
                    case "--warehouse":
                        config.warehouse = value;
                        break;
                    default:
                        // Ignore unknown args
                        break;
                }
            }
            return config;
        }

        public String getKafkaBootstrapServers() { return kafkaBootstrapServers; }
        public String getInputTopic() { return inputTopic; }
        public String getAlertTopic() { return alertTopic; }
        public String getDeadLetterTopic() { return deadLetterTopic; }
        public String getConsumerGroup() { return consumerGroup; }
        public String getRulesConfigPath() { return rulesConfigPath; }
        public String getCatalogUri() { return catalogUri; }
        public String getWarehouse() { return warehouse; }

        @Override
        public String toString() {
            return "PipelineConfig{"
                    + "kafka=" + kafkaBootstrapServers
                    + ", input=" + inputTopic
                    + ", alerts=" + alertTopic
                    + ", dead-letter=" + deadLetterTopic
                    + ", group=" + consumerGroup
                    + ", rules=" + rulesConfigPath
                    + ", catalog=" + catalogUri
                    + ", warehouse=" + warehouse
                    + '}';
        }
    }
}
