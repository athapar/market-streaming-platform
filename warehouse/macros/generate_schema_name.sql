{#-
  Schema-name override: drop dbt's default "target.schema + _ + custom" prefix.

  Default behaviour with target.schema = DBT_DEV and +schema: observability
  is to build into MARKET_STREAMING.DBT_DEV_OBSERVABILITY. That's the
  shared-Snowflake-account convention for isolating per-developer builds.

  This project has a single Snowflake account / single developer, and the
  Streamlit dashboard + downstream consumers reference the bare schema
  names (MARKET_STREAMING.OBSERVABILITY, .ANALYTICS, .MARTS, etc.). With
  this override:

      +schema: observability   ->  MARKET_STREAMING.OBSERVABILITY
      +schema: analytics       ->  MARKET_STREAMING.ANALYTICS
      (no +schema, defaults)   ->  MARKET_STREAMING.DBT_DEV   (target.schema)

  See: https://docs.getdbt.com/docs/build/custom-schemas
-#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema | trim }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
