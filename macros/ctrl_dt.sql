{#
/* this macro is used to pull the last updated time stamp from contol table */
#} {% macro ctrl_dt() %}
    {% set query %}
SELECT
    NVL(ctrl_dt,'0')
FROM
    {{ source(
        'AUDIT',
        'ABAC_JOB_CTRL'
    ) }}
WHERE
    table_name = '{{ this.table }}' {% endset %}
    {% set result = run_query(query) %}
    {% if execute %}
        {% set last_upd_time = result.columns [0].values() [0] %}
        {{ return (last_upd_time) }}
    {% endif %}
{% endmacro %}