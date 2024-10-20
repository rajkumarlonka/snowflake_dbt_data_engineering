{% macro remove_char(column_name) %}

{{ return("REGEXP_REPLACE(" ~ column_name ~ ", '[^0-9]', '')") }}

{% endmacro %}