package com.learnde.pipeline.models;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Objects;

/**
 * DeadLetterRecord POJO — represents a record routed to the dead-letter topic
 * when validation fails in the Flink stream processor.
 *
 * <p>Contains the original raw record and error details for debugging and
 * potential reprocessing. Published as JSON to the Kafka dead-letter topic.
 */
public class DeadLetterRecord implements Serializable {

    private static final long serialVersionUID = 1L;

    @JsonProperty("original_record")
    private String originalRecord;

    @JsonProperty("error_field")
    private String errorField;

    @JsonProperty("error_type")
    private String errorType;

    @JsonProperty("expected_format")
    private String expectedFormat;

    @JsonProperty("actual_value")
    private String actualValue;

    @JsonProperty("error_timestamp")
    private String errorTimestamp;

    @JsonProperty("processor_id")
    private String processorId;

    /** No-arg constructor required by Jackson. */
    public DeadLetterRecord() {}

    /**
     * Convenience constructor for creating a fully populated dead letter record.
     */
    public DeadLetterRecord(String originalRecord, String errorField, String errorType,
                            String expectedFormat, String actualValue,
                            String errorTimestamp, String processorId) {
        this.originalRecord = originalRecord;
        this.errorField = errorField;
        this.errorType = errorType;
        this.expectedFormat = expectedFormat;
        this.actualValue = actualValue;
        this.errorTimestamp = errorTimestamp;
        this.processorId = processorId;
    }

    // --- Getters and Setters ---

    /** @return the originalRecord */
    public String getOriginalRecord() {
        return originalRecord;
    }

    /** @param originalRecord the originalRecord to set */
    public void setOriginalRecord(String originalRecord) {
        this.originalRecord = originalRecord;
    }

    /** @return the errorField */
    public String getErrorField() {
        return errorField;
    }

    /** @param errorField the errorField to set */
    public void setErrorField(String errorField) {
        this.errorField = errorField;
    }

    /** @return the errorType */
    public String getErrorType() {
        return errorType;
    }

    /** @param errorType the errorType to set */
    public void setErrorType(String errorType) {
        this.errorType = errorType;
    }

    /** @return the expectedFormat */
    public String getExpectedFormat() {
        return expectedFormat;
    }

    /** @param expectedFormat the expectedFormat to set */
    public void setExpectedFormat(String expectedFormat) {
        this.expectedFormat = expectedFormat;
    }

    /** @return the actualValue */
    public String getActualValue() {
        return actualValue;
    }

    /** @param actualValue the actualValue to set */
    public void setActualValue(String actualValue) {
        this.actualValue = actualValue;
    }

    /** @return the errorTimestamp */
    public String getErrorTimestamp() {
        return errorTimestamp;
    }

    /** @param errorTimestamp the errorTimestamp to set */
    public void setErrorTimestamp(String errorTimestamp) {
        this.errorTimestamp = errorTimestamp;
    }

    /** @return the processorId */
    public String getProcessorId() {
        return processorId;
    }

    /** @param processorId the processorId to set */
    public void setProcessorId(String processorId) {
        this.processorId = processorId;
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DeadLetterRecord that = (DeadLetterRecord) o;
        return Objects.equals(originalRecord, that.originalRecord)
                && Objects.equals(errorField, that.errorField)
                && Objects.equals(errorTimestamp, that.errorTimestamp);
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        return Objects.hash(originalRecord, errorField, errorTimestamp);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return "DeadLetterRecord{"
                + "errorField='" + errorField + '\''
                + ", errorType='" + errorType + '\''
                + ", actualValue='" + actualValue + '\''
                + ", processorId='" + processorId + '\''
                + '}';
    }
}
