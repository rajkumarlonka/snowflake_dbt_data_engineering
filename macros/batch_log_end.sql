{% macro batch_log_end(batch_name) %}

{% set pull_batch_run_info %}

SELECT
batch_id,
batch_name,
batch_run_id,
batch_status
FROM {{source('AUDIT','ABAC_BATCH_RUN')}}
WHERE batch_name = '{{batch_name}}' 
AND batch_end_dt IS NULL  
ORDER BY batch_run_id DESC

{% endset %}

{% set batch_run_results = run_query(pull_batch_run_info) %}

{% if execute %}

{% set batch_id = batch_run_results.columns[0].values() [0] %}
{% set batch_name = batch_run_results.columns[1].values() [0] %}
{{ print("bbatch_name  "~batch_name) }}
{% set batch_run_id = batch_run_results.columns[2].values() [0] %}
{{ print("batch run id:  "~batch_run_id) }}
{% set batch_status = batch_run_results.columns[3].values() [0] %}

{% endif %}


{% set update_batch_run_table %}

    UPDATE {{source('AUDIT', 'ABAC_BATCH_RUN')}}
    SET batch_status = 'COMPLETED',
        batch_end_dt = CURRENT_TIMESTAMP()
    WHERE batch_name='{{batch_name}}' 
    AND batch_run_id = '{{batch_run_id}}'

{% endset %}



{% if batch_status == 'RUNNING' %}
    {% do run_query(update_batch_run_table) %}

{% endif %}

{% endmacro %}

































































































