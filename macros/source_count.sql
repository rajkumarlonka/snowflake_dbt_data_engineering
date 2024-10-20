{% macro source_count(src_table) %}

{% set src_query %}
SELECT COUNT(*) FROM {{ref(src_table)}}
{% endset %}

   {% set results = run_query(src_query) %}
{% if execute %}
    {% set count = results.columns[0].values()[0] %}
{% endif %}
{{ print("source count is : "~count) }}
{{ return(count) }}
{% endmacro %}
