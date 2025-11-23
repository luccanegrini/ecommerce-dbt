{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        -- Se o modelo não definiu +schema, usa o schema do target
        {{ target.schema }}
    {%- else -%}
        -- Se o modelo definiu +schema, usa só ele (ex: silver, gold)
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
