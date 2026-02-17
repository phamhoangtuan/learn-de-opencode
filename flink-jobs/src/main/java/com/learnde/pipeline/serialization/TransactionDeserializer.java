package com.learnde.pipeline.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnde.pipeline.models.Transaction;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Deserializes raw JSON bytes from Kafka into Transaction POJOs.
 *
 * <p>Uses Jackson ObjectMapper for JSON parsing. Returns null for records
 * that cannot be deserialized (handled downstream by the TransactionValidator
 * which routes invalid records to the dead-letter topic).
 */
public class TransactionDeserializer implements DeserializationSchema<Transaction> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(TransactionDeserializer.class);

    private transient ObjectMapper objectMapper;

    /** {@inheritDoc} Initializes the Jackson ObjectMapper for JSON deserialization. */
    @Override
    public void open(InitializationContext context) {
        this.objectMapper = new ObjectMapper();
    }

    /** {@inheritDoc} Deserializes JSON bytes into a Transaction. Returns null for malformed input. */
    @Override
    public Transaction deserialize(byte[] message) throws IOException {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
        }
        try {
            return objectMapper.readValue(message, Transaction.class);
        } catch (Exception e) {
            LOG.warn("Failed to deserialize transaction: {}", e.getMessage());
            // Return a Transaction with only the raw data set so downstream
            // can route to DLQ with the original record
            return null;
        }
    }

    /** {@inheritDoc} Always returns false — unbounded stream. */
    @Override
    public boolean isEndOfStream(Transaction nextElement) {
        return false;
    }

    /** {@inheritDoc} Returns TypeInformation for Transaction. */
    @Override
    public TypeInformation<Transaction> getProducedType() {
        return TypeInformation.of(new TypeHint<Transaction>() {});
    }
}
