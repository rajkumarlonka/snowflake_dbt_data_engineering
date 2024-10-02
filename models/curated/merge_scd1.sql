
{{
   config (
    materialized='incremental',
    database='DEV_CURATED',
    schema='CURATED',
    pre_hook="{{job_log_start()}}",
    post_hook="{{update_ctrl_dt()}}",
    unique_key='primary_unique_key',
    transient=false 

   )

}}
{% set last_upd_time = ctrl_dt() %}



WITH employee_scd1 AS (

SELECT
{{ dbt_utils.generate_surrogate_key(['employee_id']) }} AS primary_unique_key,
employee_id,
employee_name,
department,
job_title,
salary,
hire_date,
last_modified

FROM {{source('RAW','EMPLOYEE_STG')}}


{% if is_incremental %}

WHERE last_modified > '{{ last_upd_time }}'
{% endif %}

)

SELECT

primary_unique_key,
employee_id,
employee_name,
department,
job_title,
salary,
hire_date,
last_modified

FROM employee_scd1


















