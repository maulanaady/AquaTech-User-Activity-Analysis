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