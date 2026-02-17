package com.learnde.pipeline.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnde.pipeline.models.Alert;

import org.apache.flink.api.common.serialization.SerializationSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Serializes Alert POJOs to JSON bytes for publishing to Kafka alerts topic.
 *
 * <p>Uses Jackson ObjectMapper for JSON serialization.
 */
public class AlertSerializer implements SerializationSchema<Alert> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(AlertSerializer.class);

    private transient ObjectMapper objectMapper;

    /** {@inheritDoc} Initializes the Jackson ObjectMapper for JSON serialization. */
    @Override
    public void open(InitializationContext context) {
        this.objectMapper = new ObjectMapper();
    }

    /** {@inheritDoc} Serializes an Alert to JSON bytes. */
    @Override
    public byte[] serialize(Alert alert) {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
        }
        try {
            return objectMapper.writeValueAsBytes(alert);
        } catch (Exception e) {
            LOG.error("Failed to serialize alert: {}", e.getMessage());
            return new byte[0];
        }
    }
}
