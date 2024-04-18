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


-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
-- {% if var('is_test_run', default=true) %}

--   limit 100

-- {% endif %}