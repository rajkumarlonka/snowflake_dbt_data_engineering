


{{
    config(
     materialized='table',
     database='DEV_RAW',
     schema='RAW',
     pre_hook="{{job_log_start()}}"
    )

}}

with dummy as (

select '1' as id, 'rajkumar' as name
union all
select '2' as id, 'pavan' as name
union all
select '3' as id, 'rakesh' as name
)

select 
id,
name

from  dummy