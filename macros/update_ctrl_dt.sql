{# this macro IS used TO
UPDATE
    THE ctrl_dt IN abac_job_ctrl #} 
        
        {% macro update_ctrl_dt() %}
        {% set query_abac %}
    SELECT
        job_id,
        job_nm,
        job_target
    FROM
        {{ source(
            'AUDIT',
            'ABAC_JOB'
        ) }}
    WHERE
        lower(job_target) = '{{ this.table }}'
{% endset %}
        {% set results = run_query(query_abac) %}
        {% if execute %}
            {% set job_id = results.columns [0].values() [0] %}
            {% set job_name = results.columns [1].values() [0] %}
        {% endif %}

        {% set query %}
        MERGE INTO {{ source(
            'AUDIT',
            'ABAC_JOB_CTRL'
        ) }} USING (
            SELECT
            max(last_modified) AS updated_time

            FROM
                {{ this }}) AS tbl
                ON {{ source(
                    'AUDIT',
                    'ABAC_JOB_CTRL'
                ) }}.table_name = '{{ this.name }}'
                WHEN matched THEN
            UPDATE
                set ctrl_dt = tbl.updated_time
                WHEN NOT matched THEN
            INSERT
                (
                    job_id,
                    job_name,
                    table_name,
                    ctrl_dt
                )
            VALUES
                (   {{job_id}},
                    '{{job_name}}',
                    '{{this.table}}',
                    tbl.updated_time
                ) {% endset %}
                {% do run_query(query) %}
{% endmacro %}