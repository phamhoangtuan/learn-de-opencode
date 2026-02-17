package com.learnde.pipeline.functions;

import com.learnde.pipeline.models.Alert;
import com.learnde.pipeline.models.Transaction;
import com.learnde.pipeline.rules.AlertingRule;
import com.learnde.pipeline.rules.HighValueRule;
import com.learnde.pipeline.rules.RapidActivityRule;
import com.learnde.pipeline.rules.UnusualHourRule;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

/**
 * AlertEvaluator — KeyedProcessFunction that evaluates transactions against
 * all enabled alerting rules. Keyed by account_id.
 *
 * <p>Stateless rules (HighValueRule, UnusualHourRule) are evaluated immediately.
 * Stateful rules (RapidActivityRule) use Flink ValueState and event-time timers
 * to track per-account transaction counts within a rolling window.
 *
 * <p>A single transaction matching multiple rules emits multiple independent alerts,
 * each referencing the same transaction_id (spec.md edge case line 105).
 */
public class AlertEvaluator extends KeyedProcessFunction<String, Transaction, Alert> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(AlertEvaluator.class);

    private final List<AlertingRule> statelessRules;
    private final RapidActivityRule rapidActivityRule;

    // State: list of event timestamps (millis) for the current account
    private transient ListState<Long> txTimestamps;

    /**
     * Side output for enriched transactions (with is_flagged set).
     */
    public static final org.apache.flink.util.OutputTag<Transaction> ENRICHED_TX_TAG =
            new org.apache.flink.util.OutputTag<Transaction>("enriched-transactions") {};

    public AlertEvaluator(List<AlertingRule> statelessRules, RapidActivityRule rapidActivityRule) {
        this.statelessRules = statelessRules;
        this.rapidActivityRule = rapidActivityRule;
    }

    /** Default constructor with default rules. */
    public AlertEvaluator() {
        this.statelessRules = List.of(new HighValueRule(), new UnusualHourRule());
        this.rapidActivityRule = new RapidActivityRule();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ListStateDescriptor<Long> descriptor = new ListStateDescriptor<>(
                "tx-timestamps", Types.LONG);
        txTimestamps = getRuntimeContext().getListState(descriptor);
    }

    @Override
    public void processElement(Transaction tx, Context ctx, Collector<Alert> out) throws Exception {
        boolean flagged = false;

        // Evaluate stateless rules
        for (AlertingRule rule : statelessRules) {
            if (rule.isEnabled()) {
                Optional<Alert> alert = rule.evaluate(tx);
                if (alert.isPresent()) {
                    out.collect(alert.get());
                    flagged = true;
                    LOG.info("Alert generated: rule={}, txId={}", rule.getRuleName(),
                            tx.getTransactionId());
                }
            }
        }

        // Evaluate velocity rule (stateful)
        if (rapidActivityRule != null && rapidActivityRule.isEnabled()) {
            long txEventTime;
            try {
                txEventTime = ZonedDateTime.parse(tx.getTimestamp()).toInstant().toEpochMilli();
            } catch (Exception e) {
                txEventTime = ctx.timestamp() != null ? ctx.timestamp() : System.currentTimeMillis();
            }

            // Add current timestamp to state
            txTimestamps.add(txEventTime);

            // Calculate window boundary
            long windowMillis = rapidActivityRule.getWindowMinutes() * 60L * 1000L;
            long windowStart = txEventTime - windowMillis;

            // Count transactions within window, remove expired
            List<Long> validTimestamps = new ArrayList<>();
            for (Long ts : txTimestamps.get()) {
                if (ts > windowStart) {
                    validTimestamps.add(ts);
                }
            }
            txTimestamps.update(validTimestamps);

            // Register event-time timer for cleanup
            ctx.timerService().registerEventTimeTimer(txEventTime + windowMillis);

            // Check threshold
            Optional<Alert> velocityAlert = rapidActivityRule.evaluateCount(tx, validTimestamps.size());
            if (velocityAlert.isPresent()) {
                out.collect(velocityAlert.get());
                flagged = true;
                LOG.info("Alert generated: rule=rapid-activity, txId={}, count={}",
                        tx.getTransactionId(), validTimestamps.size());
            }
        }

        // Set is_flagged and emit enriched transaction via side output
        tx.setFlagged(flagged);
        ctx.output(ENRICHED_TX_TAG, tx);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) throws Exception {
        // Clean up expired timestamps from state
        if (rapidActivityRule == null) return;

        long windowMillis = rapidActivityRule.getWindowMinutes() * 60L * 1000L;
        long windowStart = timestamp - windowMillis;

        List<Long> validTimestamps = new ArrayList<>();
        for (Long ts : txTimestamps.get()) {
            if (ts > windowStart) {
                validTimestamps.add(ts);
            }
        }
        txTimestamps.update(validTimestamps);
    }
}
