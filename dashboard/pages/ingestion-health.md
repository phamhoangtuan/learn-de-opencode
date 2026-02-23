---
title: Ingestion Health
sidebar_position: 2
---

# Ingestion Health

Monitor the ingestion pipeline: records loaded, quarantined, and duplicates skipped per run.

## Latest Run Summary

```sql latest_ingestion
select
    run_id,
    started_at,
    status,
    records_loaded,
    records_quarantined,
    duplicates_skipped,
    elapsed_seconds
from warehouse.ingestion_runs
order by started_at desc
limit 1
```

<BigValue
    data={latest_ingestion}
    value=records_loaded
    title="Records Loaded"
/>
<BigValue
    data={latest_ingestion}
    value=records_quarantined
    title="Records Quarantined"
/>
<BigValue
    data={latest_ingestion}
    value=duplicates_skipped
    title="Duplicates Skipped"
/>

## Ingestion Runs History

```sql ingestion_history
select
    run_id,
    started_at,
    status,
    files_processed,
    records_loaded,
    records_quarantined,
    duplicates_skipped,
    round(elapsed_seconds, 2) as elapsed_seconds
from warehouse.ingestion_runs
order by started_at desc
```

<DataTable data={ingestion_history} rows=20>
    <Column id=run_id title="Run ID"/>
    <Column id=started_at title="Started At"/>
    <Column id=status title="Status"/>
    <Column id=files_processed title="Files"/>
    <Column id=records_loaded title="Loaded"/>
    <Column id=records_quarantined title="Quarantined"/>
    <Column id=duplicates_skipped title="Duplicates"/>
    <Column id=elapsed_seconds fmt=num2 title="Elapsed (s)"/>
</DataTable>

## Records Trend

```sql records_trend
select
    started_at,
    records_loaded,
    records_quarantined,
    duplicates_skipped
from warehouse.ingestion_runs
order by started_at
```

<LineChart
    data={records_trend}
    x=started_at
    y={['records_loaded', 'records_quarantined', 'duplicates_skipped']}
    title="Records Over Time"
/>

## Quarantine

Quarantine data will appear here when records are rejected during ingestion. Run the ingestion pipeline with invalid data to see quarantine breakdowns.
