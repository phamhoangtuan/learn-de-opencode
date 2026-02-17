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

    public String getOriginalRecord() {
        return originalRecord;
    }

    public void setOriginalRecord(String originalRecord) {
        this.originalRecord = originalRecord;
    }

    public String getErrorField() {
        return errorField;
    }

    public void setErrorField(String errorField) {
        this.errorField = errorField;
    }

    public String getErrorType() {
        return errorType;
    }

    public void setErrorType(String errorType) {
        this.errorType = errorType;
    }

    public String getExpectedFormat() {
        return expectedFormat;
    }

    public void setExpectedFormat(String expectedFormat) {
        this.expectedFormat = expectedFormat;
    }

    public String getActualValue() {
        return actualValue;
    }

    public void setActualValue(String actualValue) {
        this.actualValue = actualValue;
    }

    public String getErrorTimestamp() {
        return errorTimestamp;
    }

    public void setErrorTimestamp(String errorTimestamp) {
        this.errorTimestamp = errorTimestamp;
    }

    public String getProcessorId() {
        return processorId;
    }

    public void setProcessorId(String processorId) {
        this.processorId = processorId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DeadLetterRecord that = (DeadLetterRecord) o;
        return Objects.equals(originalRecord, that.originalRecord)
                && Objects.equals(errorField, that.errorField)
                && Objects.equals(errorTimestamp, that.errorTimestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(originalRecord, errorField, errorTimestamp);
    }

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
