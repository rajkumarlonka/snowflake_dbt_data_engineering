{% macro job_log_start() %}

{% set pull_job_info %}
SELECT
job_id,
job_nm,
job_active_flag,
batch_id,
batch_name
FROM {{source('AUDIT','ABAC_JOB')}}
 WHERE job_nm='{{this.name}}'
{% endset %}

{% set job_results = run_query(pull_job_info) %}

{% if execute %}
{% set job_id = job_results.columns[0].values()[0] %}
{% set job_nm = job_results.columns[1].values()[0] %}
{{print("job name of current: "~job_nm)}}
{% set job_active_flag = job_results.columns[2].values()[0] %}
{% set batch_id = job_results.columns[3].values()[0] %}
{% set batch_name = job_results.columns[4].values()[0] %}
{% endif %}

{% set pull_batch_run_info %}

SELECT
batch_name,
batch_id,
batch_run_id,
batch_status

FROM {{source('AUDIT','ABAC_BATCH_RUN')}}
WHERE batch_name = '{{batch_name}}'
ORDER BY batch_start_dt desc

{% endset %}

{% set batch_run_results = run_query(pull_batch_run_info) %}

{% if execute %}
{% set batch_name = batch_run_results.columns[0].values()[0] %}
{% set batch_id = batch_run_results.columns[1].values()[0] %}
{% set batch_run_id = batch_run_results.columns[2].values()[0] %}
{% set batch_status = batch_run_results.columns[3].values()[0] %}
{% endif %}


{% if job_active_flag == 'N' %}
{{exceptions.raise_compiler_error(
    "ABAC ERROR: " + job_nm + " job is not active, Please Check ABAC_JOB Table."
)}}
{% endif %}

{% if batch_status == 'COMPLETED' %}
{{exceptions.raise_compiler_error(
    "ABAC ERROR: " + batch_status + "  is COMPLETED, Please Check ABAC_BATCH_RUN Table."
)}}
{% endif %}

{% if batch_status == 'FAILED' %}
{{exceptions.raise_compiler_error(
    "ABAC ERROR: " + batch_status + "  is FAILED, Please Check ABAC_BATCH_RUN Table."
)}}
{% endif %}


{% set insert_into_job_run_table %}

INSERT INTO {{source('AUDIT','ABAC_JOB_RUN')}}
(

system_run_id,
job_id,
batch_run_id,
job_start_dt,
job_end_dt,
duration,
job_status,
created_dt,
created_by
)
VALUES
(
'{{invocation_id}}',
'{{job_id}}',
'{{batch_run_id}}',
CURRENT_TIMESTAMP(),
NULL,
NULL,
'started',
CURRENT_TIMESTAMP(),
CURRENT_USER()
)
{% endset %}

{% do run_query(insert_into_job_run_table) %}

{% endmacro %}






















































































