package com.learnde.pipeline;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Unit tests for TransactionPipeline.
 *
 * <p>Tests configuration parsing, argument handling, and pipeline setup logic.
 * Full integration testing with live Kafka/Iceberg is in T044.
 */
@DisplayName("TransactionPipeline")
class TransactionPipelineTest {

    @Nested
    @DisplayName("Configuration defaults")
    class ConfigDefaults {

        @Test
        @DisplayName("default Kafka bootstrap servers is kafka:29092")
        void defaultKafkaBootstrapServers() {
            TransactionPipeline.PipelineConfig config = TransactionPipeline.PipelineConfig.fromDefaults();
            assertThat(config.getKafkaBootstrapServers()).isEqualTo("kafka:29092");
        }

        @Test
        @DisplayName("default input topic is raw-transactions")
        void defaultInputTopic() {
            TransactionPipeline.PipelineConfig config = TransactionPipeline.PipelineConfig.fromDefaults();
            assertThat(config.getInputTopic()).isEqualTo("raw-transactions");
        }

        @Test
        @DisplayName("default alert output topic is alerts")
        void defaultAlertTopic() {
            TransactionPipeline.PipelineConfig config = TransactionPipeline.PipelineConfig.fromDefaults();
            assertThat(config.getAlertTopic()).isEqualTo("alerts");
        }

        @Test
        @DisplayName("default dead letter topic is dead-letter")
        void defaultDlqTopic() {
            TransactionPipeline.PipelineConfig config = TransactionPipeline.PipelineConfig.fromDefaults();
            assertThat(config.getDeadLetterTopic()).isEqualTo("dead-letter");
        }

        @Test
        @DisplayName("default consumer group is transaction-pipeline")
        void defaultConsumerGroup() {
            TransactionPipeline.PipelineConfig config = TransactionPipeline.PipelineConfig.fromDefaults();
            assertThat(config.getConsumerGroup()).isEqualTo("transaction-pipeline");
        }

        @Test
        @DisplayName("default rules config path is /opt/flink/config/alerting-rules.yaml")
        void defaultRulesConfigPath() {
            TransactionPipeline.PipelineConfig config = TransactionPipeline.PipelineConfig.fromDefaults();
            assertThat(config.getRulesConfigPath())
                    .isEqualTo("/opt/flink/config/alerting-rules.yaml");
        }

        @Test
        @DisplayName("default catalog URI is http://iceberg-rest:8181")
        void defaultCatalogUri() {
            TransactionPipeline.PipelineConfig config = TransactionPipeline.PipelineConfig.fromDefaults();
            assertThat(config.getCatalogUri()).isEqualTo("http://iceberg-rest:8181");
        }

        @Test
        @DisplayName("default warehouse is /warehouse")
        void defaultWarehouse() {
            TransactionPipeline.PipelineConfig config = TransactionPipeline.PipelineConfig.fromDefaults();
            assertThat(config.getWarehouse()).isEqualTo("/warehouse");
        }
    }

    @Nested
    @DisplayName("Configuration from arguments")
    class ConfigFromArgs {

        @Test
        @DisplayName("parses --kafka.bootstrap.servers from args")
        void parsesKafkaBootstrap() {
            String[] args = {"--kafka.bootstrap.servers", "localhost:9092"};
            TransactionPipeline.PipelineConfig config = TransactionPipeline.PipelineConfig.fromArgs(args);
            assertThat(config.getKafkaBootstrapServers()).isEqualTo("localhost:9092");
        }

        @Test
        @DisplayName("parses --rules.config.path from args")
        void parsesRulesConfigPath() {
            String[] args = {"--rules.config.path", "/custom/rules.yaml"};
            TransactionPipeline.PipelineConfig config = TransactionPipeline.PipelineConfig.fromArgs(args);
            assertThat(config.getRulesConfigPath()).isEqualTo("/custom/rules.yaml");
        }

        @Test
        @DisplayName("parses --catalog.uri from args")
        void parsesCatalogUri() {
            String[] args = {"--catalog.uri", "http://localhost:8181"};
            TransactionPipeline.PipelineConfig config = TransactionPipeline.PipelineConfig.fromArgs(args);
            assertThat(config.getCatalogUri()).isEqualTo("http://localhost:8181");
        }

        @Test
        @DisplayName("empty args uses defaults")
        void emptyArgsUsesDefaults() {
            String[] args = {};
            TransactionPipeline.PipelineConfig config = TransactionPipeline.PipelineConfig.fromArgs(args);
            assertThat(config.getKafkaBootstrapServers()).isEqualTo("kafka:29092");
            assertThat(config.getInputTopic()).isEqualTo("raw-transactions");
        }

        @Test
        @DisplayName("unknown args are ignored")
        void unknownArgsIgnored() {
            String[] args = {"--unknown.param", "value"};
            assertThatCode(() -> TransactionPipeline.PipelineConfig.fromArgs(args))
                    .doesNotThrowAnyException();
        }
    }

    @Nested
    @DisplayName("Pipeline config validation")
    class ConfigValidation {

        @Test
        @DisplayName("config toString includes all key settings")
        void toStringIncludesSettings() {
            TransactionPipeline.PipelineConfig config = TransactionPipeline.PipelineConfig.fromDefaults();
            String str = config.toString();
            assertThat(str).contains("kafka:29092");
            assertThat(str).contains("raw-transactions");
            assertThat(str).contains("alerts");
            assertThat(str).contains("dead-letter");
        }
    }
}
