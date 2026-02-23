---
title: Pipeline Overview
sidebar_position: 0
---

# Pipeline Overview

At-a-glance health summary for the data pipeline: ingestion, transformation, and quality checks.

## Latest Pipeline Status

```sql latest_ingestion
select
    status,
    records_loaded,
    started_at
from warehouse.ingestion_runs
order by started_at desc
limit 1
```

```sql latest_transform
select
    status,
    models_executed,
    started_at
from warehouse.transform_runs
order by started_at desc
limit 1
```

```sql latest_check
select
    status,
    checks_passed,
    total_checks,
    started_at
from warehouse.check_runs
order by started_at desc
limit 1
```

<BigValue
    data={latest_ingestion}
    value=records_loaded
    title="Ingestion: Records Loaded"
/>
<BigValue
    data={latest_transform}
    value=models_executed
    title="Transform: Models Executed"
/>
<BigValue
    data={latest_check}
    value=checks_passed
    title="Quality: Checks Passed"
/>

## Pipeline Timeline

```sql pipeline_timeline
select
    'ingestion' as run_type,
    run_id,
    started_at,
    status,
    records_loaded || ' records' as key_metric,
    round(elapsed_seconds, 2) as elapsed_seconds
from warehouse.ingestion_runs

union all

select
    'transform' as run_type,
    run_id,
    started_at,
    status,
    models_executed || ' models' as key_metric,
    round(elapsed_seconds, 2) as elapsed_seconds
from warehouse.transform_runs

union all

select
    'check' as run_type,
    run_id,
    started_at,
    status,
    checks_passed || '/' || total_checks || ' passed' as key_metric,
    round(elapsed_seconds, 2) as elapsed_seconds
from warehouse.check_runs

order by started_at desc
```

<DataTable data={pipeline_timeline} rows=20>
    <Column id=run_type title="Type"/>
    <Column id=started_at title="Started At"/>
    <Column id=status title="Status"/>
    <Column id=key_metric title="Key Metric"/>
    <Column id=elapsed_seconds fmt=num2 title="Elapsed (s)"/>
</DataTable>
