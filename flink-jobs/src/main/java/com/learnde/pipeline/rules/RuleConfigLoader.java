package com.learnde.pipeline.rules;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;

/**
 * RuleConfigLoader — loads alerting rules from a YAML configuration file.
 *
 * <p>Reads {@code config/alerting-rules.yaml} and instantiates the corresponding
 * rule objects (HighValueRule, RapidActivityRule, UnusualHourRule) based on the
 * {@code rule_type} field.
 *
 * <p>Supports hot-reload: call {@link #loadRules()} again to re-read the file.
 * The loader is serializable so it can be passed to Flink operators.
 */
public class RuleConfigLoader implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(RuleConfigLoader.class);

    private final String configPath;

    /**
     * Create a RuleConfigLoader pointing at a specific YAML file.
     *
     * @param configPath absolute or relative path to alerting-rules.yaml
     */
    public RuleConfigLoader(String configPath) {
        this.configPath = configPath;
    }

    /** Default constructor using {@code /opt/flink/config/alerting-rules.yaml}. */
    public RuleConfigLoader() {
        this("/opt/flink/config/alerting-rules.yaml");
    }

    /**
     * Load all alerting rules from the YAML config file.
     *
     * <p>Returns only the stateless rules (HighValueRule, UnusualHourRule).
     * RapidActivityRule is returned separately via {@link #loadRapidActivityRule()}.
     *
     * @return list of stateless AlertingRule instances
     */
    public List<AlertingRule> loadStatelessRules() {
        List<AlertingRule> rules = new ArrayList<>();

        try {
            JsonNode root = parseYaml();
            if (root == null || !root.has("rules")) {
                LOG.warn("No 'rules' key found in config file: {}", configPath);
                return getDefaultStatelessRules();
            }

            JsonNode rulesNode = root.get("rules");
            for (JsonNode ruleNode : rulesNode) {
                String ruleType = ruleNode.path("rule_type").asText("");
                boolean enabled = ruleNode.path("enabled").asBoolean(true);
                String severity = ruleNode.path("severity").asText("medium");
                JsonNode params = ruleNode.path("parameters");

                switch (ruleType) {
                    case "threshold":
                        BigDecimal threshold = new BigDecimal(
                                params.path("amount_threshold").asText("10000"));
                        rules.add(new HighValueRule(threshold, severity, enabled));
                        LOG.info("Loaded rule: high-value-transaction, threshold={}, severity={}, enabled={}",
                                threshold, severity, enabled);
                        break;

                    case "time_based":
                        LocalTime quietStart = LocalTime.parse(
                                params.path("quiet_start").asText("01:00"));
                        LocalTime quietEnd = LocalTime.parse(
                                params.path("quiet_end").asText("05:00"));
                        rules.add(new UnusualHourRule(quietStart, quietEnd, severity, enabled));
                        LOG.info("Loaded rule: unusual-hour, quietStart={}, quietEnd={}, severity={}, enabled={}",
                                quietStart, quietEnd, severity, enabled);
                        break;

                    case "velocity":
                        // Velocity rules are stateful — loaded separately
                        break;

                    default:
                        LOG.warn("Unknown rule_type '{}', skipping", ruleType);
                        break;
                }
            }
        } catch (Exception e) {
            LOG.error("Failed to load rules from {}, using defaults", configPath, e);
            return getDefaultStatelessRules();
        }

        if (rules.isEmpty()) {
            LOG.warn("No stateless rules loaded from config, using defaults");
            return getDefaultStatelessRules();
        }

        return rules;
    }

    /**
     * Load the RapidActivityRule from the YAML config file.
     *
     * @return the configured RapidActivityRule, or default if not found
     */
    public RapidActivityRule loadRapidActivityRule() {
        try {
            JsonNode root = parseYaml();
            if (root == null || !root.has("rules")) {
                return new RapidActivityRule();
            }

            for (JsonNode ruleNode : root.get("rules")) {
                String ruleType = ruleNode.path("rule_type").asText("");
                if ("velocity".equals(ruleType)) {
                    boolean enabled = ruleNode.path("enabled").asBoolean(true);
                    String severity = ruleNode.path("severity").asText("medium");
                    JsonNode params = ruleNode.path("parameters");

                    int maxCount = Integer.parseInt(params.path("max_count").asText("5"));
                    int windowMinutes = Integer.parseInt(params.path("window_minutes").asText("1"));

                    LOG.info("Loaded rule: rapid-activity, maxCount={}, windowMinutes={}, severity={}, enabled={}",
                            maxCount, windowMinutes, severity, enabled);
                    return new RapidActivityRule(maxCount, windowMinutes, severity, enabled);
                }
            }
        } catch (Exception e) {
            LOG.error("Failed to load rapid-activity rule from {}, using default", configPath, e);
        }

        return new RapidActivityRule();
    }

    /**
     * Load all rules from the config file (convenience method).
     *
     * @return list of all AlertingRule instances (stateless + velocity)
     */
    public List<AlertingRule> loadAllRules() {
        List<AlertingRule> allRules = new ArrayList<>(loadStatelessRules());
        allRules.add(loadRapidActivityRule());
        return allRules;
    }

    /**
     * Get the config file path.
     *
     * @return the path to the alerting-rules.yaml file
     */
    public String getConfigPath() {
        return configPath;
    }

    private JsonNode parseYaml() throws Exception {
        File file = new File(configPath);
        if (!file.exists()) {
            LOG.warn("Config file not found: {}", configPath);
            return null;
        }

        ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
        return yamlMapper.readTree(file);
    }

    private List<AlertingRule> getDefaultStatelessRules() {
        List<AlertingRule> defaults = new ArrayList<>();
        defaults.add(new HighValueRule());
        defaults.add(new UnusualHourRule());
        return defaults;
    }
}
