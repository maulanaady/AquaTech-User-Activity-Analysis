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