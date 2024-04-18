CREATE TABLE event_zoomcamp_dataset.event_data (
    distinct_id INTEGER NOT NULL,
    elements STRING,
    event STRING,
    event_time TIMESTAMP NOT NULL,
    file STRING,
    geoip_city_name STRING,
    geoip_continent_code STRING,
    geoip_continent_name STRING,
    geoip_country_code STRING,
    geoip_country_name STRING,
    geoip_latitude FLOAT64,
    geoip_longitude FLOAT64,
    geoip_subdivision_1_code STRING,
    geoip_subdivision_1_name STRING,
    geoip_time_zone STRING,
    uuid STRING,
    dl_updated_at TIMESTAMP NOT NULL
)
PARTITION BY DATE(event_time)
CLUSTER BY dl_updated_at;



CREATE TABLE event_zoomcamp_dataset.dau (
    event_date DATE NOT NULL,
    cnt INTEGER,
    dl_updated_at TIMESTAMP NOT NULL
)
PARTITION BY DATE_TRUNC(event_date,MONTH);

insert into event_zoomcamp_dataset.dau 
values(date(timestamp '2022-01-01 00:00:00'),0,timestamp '2022-01-01 00:00:00');

CREATE TABLE event_zoomcamp_dataset.mau (
    event_month DATE NOT NULL,
    cnt INTEGER,
    dl_updated_at TIMESTAMP NOT NULL
);

insert into event_zoomcamp_dataset.mau 
values(date(timestamp '2022-01-01 00:00:00'),0,timestamp '2022-01-01 00:00:00');

-- merge into event_zoomcamp_dataset.mau T
-- using
-- (with execution_time as 
-- (
--  SELECT min(dl_updated_at) as runtime
--  from event_zoomcamp_dataset.event_data
-- ),
-- execution_date as
-- (
--     select date(runtime) as run_date
--     from execution_time
-- ),
-- loaded_data as (
--   select distinct DATE_TRUNC(event_time, MONTH) as tmp_event_month
--   from execution_date b
--   inner join event_zoomcamp_dataset.event_data a
--   on b.run_date = DATE(a.dl_updated_at)
-- )
-- select DATE(DATE_TRUNC(event_time, MONTH)) as event_month, count(distinct distinct_id) as cnt,
-- c.runtime as dl_updated_at
-- from loaded_data b
-- inner join event_zoomcamp_dataset.event_data a
-- on b.tmp_event_month = DATE_TRUNC(a.event_time, MONTH)
-- inner join execution_time c on 1=1
-- group by 1,3
-- order by 1
-- ) S
-- on T.event_month = S.event_month
-- WHEN NOT MATCHED BY TARGET THEN
--   INSERT ROW
-- WHEN MATCHED THEN UPDATE
--   SET T.cnt = S.cnt,
--   T.dl_updated_at = S.dl_updated_at;



-- merge into event_zoomcamp_dataset.dau T
-- using
-- (
-- with execution_date as 
-- (
--  SELECT min(dl_updated_at) as runtime
--  from event_zoomcamp_dataset.event_data
-- ),
-- eventdate as 
-- (
--   select distinct date(event_time) as diff_date
--   from execution_date b
--   inner join event_zoomcamp_dataset.event_data a
--   on date(a.dl_updated_at) = date(b.runtime)
-- )


-- select
--     date(a.event_time) as event_date, count(distinct distinct_id) as cnt, runtime as dl_updated_at
-- from eventdate b
-- inner join event_zoomcamp_dataset.event_data a
-- on b.diff_date = date(a.event_time)
-- inner join execution_date on 1 = 1
-- group by 1,3
-- ) S
-- on T.event_date = S.event_date
-- WHEN NOT MATCHED BY TARGET THEN
--   INSERT ROW

