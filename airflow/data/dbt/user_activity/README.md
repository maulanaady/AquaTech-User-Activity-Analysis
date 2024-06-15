### SQL Manipulation to Handle Late Arriving Data

These SQL models are used within dbt (data build tool) to handle the processing of daily active user (*DAU*) and monthly active user (*MAU*) metrics, incorporating late arriving data. Each model has a specific role in the ETL pipeline. Let's break down each SQL model and understand its purpose and logic.

1. *dau_event_data.sql*

This model calculates the daily active users from the raw event data, ensuring that both new and late arriving data are included.
```
{{
    config(
        materialized='ephemeral'
    )
}}

with execution_date as 
(
    SELECT TIMESTAMP('{{ var("runtime") }}') as runtime
),
eventdate as 
(
    select distinct date(event_time) as diff_date
    from execution_date b
    inner join {{ source('staging','event_data') }} a
    on date(a.dl_updated_at) = date(b.runtime)
)

select
    date(a.event_time) as event_date, count(distinct distinct_id) as cnt, runtime as dl_updated_at
from eventdate b
inner join {{ source('staging','event_data') }} a
on b.diff_date = date(a.event_time)
inner join execution_date on 1 = 1
group by 1,3
```

Explanation:

+ Materialized as ephemeral: This means the model is not persisted as a table but is used as a CTE (Common Table Expression) or subquery.
+ execution_date CTE: Captures the runtime when the model is executed.
+ eventdate CTE: Selects unique dates from event_time based on the dl_updated_at date.
+ Final Query: Joins eventdate with the raw event data to count distinct user IDs (distinct_id) per event date, including the runtime for late arriving data tracking.

2. *mau_event_data.sql*

This model calculates the monthly active users from the raw event data, ensuring inclusion of late arriving data.
```
{{
    config(
        materialized='ephemeral'
    )
}}

with execution_time as 
(
    SELECT TIMESTAMP('{{ var("runtime") }}') as runtime
),
execution_date as
(
    select date(runtime) as run_date
    from execution_time
),
loaded_data as (
    select distinct {{ date_trunc("month", "event_time") }} as tmp_event_month
    from execution_date b
    inner join {{ source('staging','event_data') }} a
    on b.run_date = date(a.dl_updated_at)
)

select DATE({{ date_trunc("month", "event_time") }}) as event_month, 
count(distinct distinct_id) as cnt, c.runtime as dl_updated_at
from loaded_data b
inner join {{ source('staging','event_data') }} a
on b.tmp_event_month = {{ date_trunc("month", "a.event_time") }}
inner join execution_time c on 1=1
group by 1,3
```

Explanation:

+ Materialized as ephemeral: Similar to the dau_event_data.sql model.
+ execution_time and execution_date CTEs: Capture the runtime and corresponding date.
+ loaded_data CTE: Selects unique months from event_time based on dl_updated_at.
+ Final Query: Joins loaded_data with the raw event data to count distinct user IDs per month, including the runtime for tracking late arriving data.

3. *dau.sql*

This model materializes the daily active users' data incrementally, meaning it only processes new or updated data since the last run.
```
{{
    config(
        materialized='incremental',
        unique_key='event_date',
    )
}}

select *
from {{ ref('dau_event_data') }}

{% if is_incremental() %}
where dl_updated_at >= (select max(dl_updated_at) from {{ this }})
{% endif %}
```

Explanation:

+ Materialized as incremental: This means the table is updated incrementally with new or changed data.
+ Unique Key: event_date ensures data uniqueness.
+ Incremental Logic: If running incrementally, only select records where dl_updated_at is greater than or equal to the maximum dl_updated_at in the existing table.

4. *mau.sql*

This model materializes the monthly active users' data incrementally, similar to `dau.sql`.
```
{{
    config(
        materialized='incremental',
        unique_key='event_month',
    )
}}

select *
from {{ ref('mau_event_data') }}

{% if is_incremental() %}
where dl_updated_at >= (select max(dl_updated_at) from {{ this }})
{% endif %}
```

Explanation:

+ Materialized as incremental: Similar to the dau.sql model.
+ Unique Key: event_month ensures data uniqueness.
+ Incremental Logic: If running incrementally, only select records where dl_updated_at is greater than or equal to the maximum dl_updated_at in the existing table.


The incremental models (dau.sql and mau.sql) efficiently update the data, reducing processing time and resources. This approach not only enhances data accuracy but also optimizes the ETL process.