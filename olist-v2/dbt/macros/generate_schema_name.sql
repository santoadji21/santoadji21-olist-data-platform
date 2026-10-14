{# Model mendarat TEPAT di schema yang dinyatakan (staging / dw / mart), bukan "<schema_target>_<schema_model>" bawaan dbt. #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}{{ target.schema }}{%- else -%}{{ custom_schema_name | trim }}{%- endif -%}
{%- endmacro %}
