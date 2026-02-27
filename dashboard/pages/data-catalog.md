---
title: Data Catalog
sidebar_position: 4
---

# Data Catalog

Browse all warehouse tables, views, and their column definitions.

## Warehouse Overview

```sql table_summary
select
    zone,
    count(*) as table_count,
    sum(coalesce(row_count, 0)) as total_rows,
    sum(column_count) as total_columns
from warehouse.catalog_tables
group by zone
order by zone
```

<DataTable data={table_summary}>
    <Column id=zone title="Zone"/>
    <Column id=table_count title="Tables"/>
    <Column id=total_rows title="Total Rows" fmt=num0/>
    <Column id=total_columns title="Total Columns"/>
</DataTable>

## Zone Distribution

```sql zone_chart
select
    zone,
    count(*) as table_count
from warehouse.catalog_tables
group by zone
order by zone
```

<BarChart
    data={zone_chart}
    x=zone
    y=table_count
    title="Tables by Zone"
/>

## All Tables

```sql all_tables
select
    table_name,
    table_type,
    zone,
    coalesce(description, '') as description,
    row_count,
    column_count,
    coalesce(depends_on, '') as depends_on
from warehouse.catalog_tables
order by zone, table_name
```

<DataTable data={all_tables} rows=25>
    <Column id=table_name title="Table"/>
    <Column id=zone title="Zone"/>
    <Column id=table_type title="Type"/>
    <Column id=description title="Description"/>
    <Column id=row_count title="Rows" fmt=num0/>
    <Column id=column_count title="Columns"/>
    <Column id=depends_on title="Depends On"/>
</DataTable>

## Column Explorer

```sql all_columns
select
    c.table_name,
    t.zone,
    c.column_name,
    c.data_type,
    c.is_nullable,
    c.is_pk,
    coalesce(c.description, '') as description,
    coalesce(c.sample_values, '') as sample_values
from warehouse.catalog_columns c
inner join warehouse.catalog_tables t on c.table_name = t.table_name
order by t.zone, c.table_name, c.ordinal_position
```

<DataTable data={all_columns} rows=50>
    <Column id=table_name title="Table"/>
    <Column id=zone title="Zone"/>
    <Column id=column_name title="Column"/>
    <Column id=data_type title="Type"/>
    <Column id=is_nullable title="Nullable"/>
    <Column id=is_pk title="PK"/>
    <Column id=description title="Description"/>
    <Column id=sample_values title="Samples"/>
</DataTable>
