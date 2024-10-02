
{{
   config (
    materialized='incremental',
    database='DEV_CURATED',
    schema='CURATED',
    transient=false 

   )

}}

select * from {{ ref('employee_scd2_snapshot') }}














