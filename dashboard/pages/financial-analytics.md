---
title: Financial Analytics
sidebar_position: 1
---

# Financial Analytics

Analysis of spending patterns across categories, accounts, merchants, and currencies.

## Daily Spend by Category

```sql daily_spend
select
    transaction_date,
    category,
    total_amount,
    transaction_count
from warehouse.daily_spend_by_category
order by transaction_date, category
```

<BarChart
    data={daily_spend}
    x=transaction_date
    y=total_amount
    series=category
    title="Daily Spend by Category"
    yFmt=num2
/>

## Monthly Account Summary

```sql monthly_summary
select
    month,
    account_id,
    currency,
    total_debits,
    total_credits,
    net_flow,
    transaction_count
from warehouse.monthly_account_summary
order by month desc, account_id
```

<DataTable data={monthly_summary} rows=20>
    <Column id=month fmt=yyyy-mm title="Month"/>
    <Column id=account_id title="Account"/>
    <Column id=currency title="Currency"/>
    <Column id=total_debits fmt=num2 title="Total Debits"/>
    <Column id=total_credits fmt=num2 title="Total Credits"/>
    <Column id=net_flow fmt=num2 title="Net Flow"/>
    <Column id=transaction_count title="Transactions"/>
</DataTable>

## Top Merchants by Spend

```sql top_merchants
select
    merchant_name,
    sum(amount) as total_amount,
    count(*) as transaction_count
from warehouse.transactions
where transaction_type = 'debit'
group by merchant_name
order by total_amount desc
limit 10
```

<BarChart
    data={top_merchants}
    x=merchant_name
    y=total_amount
    swapXY=true
    title="Top 10 Merchants by Total Spend"
    yFmt=num2
/>

## Star Schema: Fact Transactions

```sql fct_summary
select
    account_sk,
    account_id,
    count(*) as transaction_count,
    sum(amount) as total_amount,
    min(transaction_date) as first_transaction,
    max(transaction_date) as last_transaction
from warehouse.fct_transactions
group by account_sk, account_id
order by total_amount desc
```

<DataTable data={fct_summary} rows=20>
    <Column id=account_sk title="Account SK"/>
    <Column id=account_id title="Account ID"/>
    <Column id=transaction_count title="Transactions"/>
    <Column id=total_amount fmt=num2 title="Total Amount"/>
    <Column id=first_transaction title="First Txn"/>
    <Column id=last_transaction title="Last Txn"/>
</DataTable>

## Currency Breakdown

```sql currency_breakdown
select
    currency,
    count(*) as transaction_count,
    sum(amount) as total_amount,
    round(avg(amount), 2) as avg_amount
from warehouse.transactions
group by currency
order by total_amount desc
```

<BarChart
    data={currency_breakdown}
    x=currency
    y=total_amount
    title="Total Amount by Currency"
    yFmt=num2
/>

<BarChart
    data={currency_breakdown}
    x=currency
    y=transaction_count
    title="Transaction Count by Currency"
/>

<DataTable data={currency_breakdown}>
    <Column id=currency title="Currency"/>
    <Column id=transaction_count title="Transactions"/>
    <Column id=total_amount fmt=num2 title="Total Amount"/>
    <Column id=avg_amount fmt=num2 title="Avg Amount"/>
</DataTable>
