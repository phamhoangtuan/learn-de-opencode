package com.learnde.pipeline.rules;

import com.learnde.pipeline.models.Alert;
import com.learnde.pipeline.models.Transaction;

import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.Optional;

/**
 * UnusualHourRule — flags transactions occurring during configurable quiet hours.
 *
 * <p>Rule type: time_based. Default quiet period: 01:00-05:00 UTC.
 * Generates a "low" severity alert when triggered.
 */
public class UnusualHourRule implements AlertingRule {

    private static final long serialVersionUID = 1L;

    private final LocalTime quietStart;
    private final LocalTime quietEnd;
    private final String severity;
    private final boolean enabled;

    /** Constructs an UnusualHourRule. @param quietStart start of quiet hours @param quietEnd end of quiet hours @param severity alert severity level @param enabled whether this rule is active */
    public UnusualHourRule(LocalTime quietStart, LocalTime quietEnd,
                           String severity, boolean enabled) {
        this.quietStart = quietStart;
        this.quietEnd = quietEnd;
        this.severity = severity;
        this.enabled = enabled;
    }

    /** Default constructor with 01:00-05:00 UTC quiet hours. */
    public UnusualHourRule() {
        this(LocalTime.of(1, 0), LocalTime.of(5, 0), "low", true);
    }

    /** {@inheritDoc} */
    @Override
    public String getRuleName() {
        return "unusual-hour";
    }

    /** {@inheritDoc} */
    @Override
    public String getSeverity() {
        return severity;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isEnabled() {
        return enabled;
    }

    /** @return start time of the quiet hours period */
    public LocalTime getQuietStart() {
        return quietStart;
    }

    /** @return end time of the quiet hours period */
    public LocalTime getQuietEnd() {
        return quietEnd;
    }

    /** {@inheritDoc} */
    @Override
    public Optional<Alert> evaluate(Transaction transaction) {
        if (!enabled || transaction.getTimestamp() == null) {
            return Optional.empty();
        }

        try {
            ZonedDateTime txTime = ZonedDateTime.parse(transaction.getTimestamp())
                    .withZoneSameInstant(ZoneOffset.UTC);
            LocalTime txLocalTime = txTime.toLocalTime();

            boolean inQuietHours;
            if (quietStart.isBefore(quietEnd)) {
                // Normal range: e.g., 01:00-05:00
                inQuietHours = !txLocalTime.isBefore(quietStart) && txLocalTime.isBefore(quietEnd);
            } else {
                // Wrapping range: e.g., 22:00-06:00
                inQuietHours = !txLocalTime.isBefore(quietStart) || txLocalTime.isBefore(quietEnd);
            }

            if (inQuietHours) {
                String description = String.format(
                        "Transaction at %s UTC falls within quiet hours (%s-%s)",
                        txLocalTime.toString(), quietStart.toString(), quietEnd.toString());
                Alert alert = new Alert(
                        transaction.getTransactionId(),
                        getRuleName(),
                        severity,
                        Instant.now().toString(),
                        description);
                return Optional.of(alert);
            }
        } catch (DateTimeParseException e) {
            // Cannot evaluate if timestamp is unparseable
            return Optional.empty();
        }

        return Optional.empty();
    }
}
