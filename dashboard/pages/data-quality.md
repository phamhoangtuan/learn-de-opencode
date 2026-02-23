---
title: Data Quality
sidebar_position: 3
---

# Data Quality

Track data quality check results: pass/fail/warn status per check, severity levels, and violation details.

## Latest Run Summary

```sql latest_check_run
select
    run_id,
    started_at,
    status,
    total_checks,
    checks_passed,
    checks_failed,
    checks_errored,
    round(elapsed_seconds, 2) as elapsed_seconds
from warehouse.check_runs
order by started_at desc
limit 1
```

<BigValue
    data={latest_check_run}
    value=checks_passed
    title="Checks Passed"
/>
<BigValue
    data={latest_check_run}
    value=checks_failed
    title="Checks Failed"
/>
<BigValue
    data={latest_check_run}
    value=checks_errored
    title="Checks Errored"
/>

## Check Results from Latest Run

```sql latest_results
select
    cr.check_name,
    cr.severity,
    cr.status,
    cr.violation_count,
    cr.description
from warehouse.check_results cr
inner join (
    select run_id
    from warehouse.check_runs
    order by started_at desc
    limit 1
) latest on cr.run_id = latest.run_id
order by cr.severity, cr.check_name
```

<DataTable data={latest_results} rows=20>
    <Column id=check_name title="Check Name"/>
    <Column id=severity title="Severity"/>
    <Column id=status title="Status"/>
    <Column id=violation_count title="Violations"/>
    <Column id=description title="Description"/>
</DataTable>

## Check Outcomes Over Time

```sql outcomes_over_time
select
    started_at,
    checks_passed,
    checks_failed,
    checks_errored
from warehouse.check_runs
order by started_at
```

<BarChart
    data={outcomes_over_time}
    x=started_at
    y={['checks_passed', 'checks_failed', 'checks_errored']}
    title="Check Outcomes Over Time"
/>

## Check Results Detail

```sql all_check_results
select
    cr.run_id,
    cr.check_name,
    cr.severity,
    cr.status,
    cr.violation_count,
    round(cr.elapsed_seconds, 4) as elapsed_seconds
from warehouse.check_results cr
order by cr.run_id desc, cr.check_name
```

<DataTable data={all_check_results} rows=20>
    <Column id=run_id title="Run ID"/>
    <Column id=check_name title="Check Name"/>
    <Column id=severity title="Severity"/>
    <Column id=status title="Status"/>
    <Column id=violation_count title="Violations"/>
    <Column id=elapsed_seconds fmt=num4 title="Elapsed (s)"/>
</DataTable>
