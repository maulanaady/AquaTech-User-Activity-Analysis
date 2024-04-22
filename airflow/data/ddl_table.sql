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
