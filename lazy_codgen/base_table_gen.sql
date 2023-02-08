{% macro base_table_gen(source_name, table_name, partition_field=None, timezone=None, materialized=None) %}
  {{ return(adapter.dispatch('base_table_gen')(source_name, table_name,partition_field, timezone, materialized)) }}
{% endmacro %}
{% macro bigquery__base_table_gen (source_name, table_name, partition_field, timezone, materialized) %}
{% set source_relation = source(source_name, table_name) %}
{% set columns = adapter.get_columns_in_relation(source_relation) %}
{% set re = modules.re %}
  {% set column_boolean = [] %}
  {% set column_number = [] %}
  {% set column_string = [] %}
  {% set column_date = [] %}
  {% set column_datetime = [] %}
  {% set column_json = [] %}
  {% set column_others = [] %}

{% for column in columns %}

    {% if column.dtype in ('INT64','FLOAT64','NUMERIC','BIGNUMERIC') and column_name not in column_id %}
      
      {% do column_number.append(column.name|lower) %}

    {% elif column.dtype in 'STRING' and column_name not in string_id %}

      {% do column_string.append(column.name|lower) %}

    {% elif column.dtype in 'BOOLEAN' %}
      
      {% do column_boolean.append(column.name|lower) %}

    {% elif column.dtype in ('DATE') %}

      {% do column_date.append(column.name|lower) %}

     {% elif column.dtype in ('DATETIME','TIMESTAMP') %}

      {% do column_datetime.append(column.name|lower) %}
    {% elif column.dtype in ('JSON') %}
       {% do column_json.append(column.name|lower) %}  
  
    {% else %}
      {% do column_others.append(column.name|lower) %}
    {% endif %}
{% endfor %}

{% set base_model_sql %}
             
{%- if materialized is not none -%}
    {{ "{{ config(materialized='" ~ materialized ~ "') }}" }}
{%- endif %}

with source as (

    select * from {% raw %}{{ source({% endraw %}'{{ source_name }}', '{{ table_name }}'{% raw %}) }}{% endraw %}
    {%- if partition_field is not none %}
    {% raw %}{% if target.name == 'dev' %}{% endraw %}
    where  {{ partition_field }} >= date_sub(current_date, interval 3 day) 
    {% raw %}{% endif %}{% endraw %}
    {%-endif %}

),



renamed as (

    select
        --string column
        {%- for dimension in column_string -%}
        {{"," if not loop.first}}
        {{ dimension }}
        {%- endfor -%}
    
        {%- if column_date|length > 0 %}
        --date column 
            {%- for date in column_date  -%}
        ,
        {{date}}
            {%- endfor -%}
        {%- endif %}        
        
        {%- if column_datetime|length > 0 %}
        --datetime columns
            {%- for datetime in column_datetime -%}
        ,
        datetime({{datetime}} {{ ",timezone" if timezone is not none }} ) as {{ datetime }}
            {%- endfor -%}
        {%- endif %}        
        {%- if column_number|length > 0 %}
        --number columns
            {%- for fct in column_number -%}
        ,
        {{ fct }}
            {%- endfor -%}
        {%- endif -%}
        
        {%- if column_json|length > 0 %}
        --json columns
            {%- for json_value in column_json %}
        {{ json_value }}
            {%- endfor -%}
        {%- endif %}
                  
        {%- if column_boolean|length > 0 %}
        --boolean columns
            {%- for boolean_v in column_boolean -%}
        ,            
        {{boolean_v}} as is_{[ boolean_v ]} 
            {%- endfor -%}
        {%- endif %}                    
      

    from source

)

select * from renamed
{% endset %}

{% if execute %}

{{ log(base_model_sql, info=True) }}
{% do return(base_model_sql) %}

{% endif %}
{% endmacro %}


{% macro snowflake__base_table_gen (source_name, table_name,partition_field, timezone, materialized) %}
{% set source_relation = source(source_name, table_name) %}
{% set columns = adapter.get_columns_in_relation(source_relation) %}
{% set re = modules.re %}
  {% set column_id = [] %}
  {% set string_id = []%}
  {% set column_boolean = [] %}
  {% set column_number = [] %}
  {% set column_string = [] %}
  {% set column_date = [] %}
  {% set column_datetime = [] %}
  {% set column_json = [] %}
  {% set column_others = [] %}

{% for column in columns %}
    {% if column.dtype in ('INT','NUMBER','DECIMAL','NUMERIC','BIGNUMERIC','FLOAT','DOUBLE') %}
      
      {% do column_number.append(column.name|lower) %}

    {% elif column.dtype in ('STRING', 'VARCHAR','CHAR','CHARACTER','TEXT')  %}

      {% do column_string.append(column.name|lower) %}

    {% elif column.dtype in 'BOOLEAN' %}
      
      {% do column_boolean.append(column.name|lower) %}

    {% elif column.dtype in ('DATE') %}
('DATETIME','TIMESTAMP')
      {% do column_date.append(column.name|lower) %}

     {% elif column.dtype in ('DATETIME','TIMESTAMP') %}

      {% do column_datetime.append(column.name|lower) %}
    {% elif column.dtype in ('JSON') %}
       {% do column_json.append(column.name|lower) %}  
      
    {% else %}
      {% do column_others.append(column.name|lower) %}
    {% endif %}
{% endfor %}
{% set test_query %}
         select * from  (select
        {% for column in column_string %}
            
            lower({{ column }}) as {{column}}  {{ "," if not loop.last }}
       {% endfor %}
            from {{ source_relation }} 
            where 1=1 
            {% if test_partitioning is not none %}
            and {{ test_partitioning }}
            {% endif %}
            limit 1 
         )
         unpivot ( context for column_name in ( {% for column in column_string %} {{column}} {{ "," if not loop.last }} {% endfor %}  ) )

{% endset %}

{% set base_model_sql %}
               
{%- if materialized is not none -%}
    {{ "{{ config(materialized='" ~ materialized ~ "') }}" }}
{%- endif %}

with source as (

    
    select * from {% raw %}{{ source({% endraw %}'{{ source_name }}', '{{ table_name }}'{% raw %}) }}{% endraw %}
    {%- if partition_field is not none %}
    {% raw %}{% if target.name == 'dev' %}{% endraw %}
    where  {{ partition_field }} >= date_add(day, -3, current_date()) 
    {% raw %}{% endif %}{% endraw %}
    {%-endif %}

),



renamed as (

    select
         --string column
        {%- for dimension in column_string -%}
        {{"," if not loop.first}}
        {{ dimension }}
        {%- endfor -%}
        {%- if column_date|length > 0 %}
         --date column 
            {%- for date in column_date  -%}
        ,
        {{date}}
            {%- endfor -%}
        {%- endif %}        
        {%- if column_datetime|length > 0 %}
         --datetime column 
            {%- for datetime in column_datetime -%}
        ,
        datetime({{datetime}} {{ ",timezone" if timezone is not none }} ) as {{ datetime }}
            {%- endfor -%}
        {%- endif %}        
        {%- if column_number|length > 0 %}
          --number column
            {%- for fct in column_number -%}
        ,
        {{ fct }}
            {%- endfor -%}
        {%- endif -%}
        
        {%- if column_json|length > 0 %}
         --json column
            {%- for json_value in column_json -%}
        {{ json_value }}
            {%- endfor -%}
        {%- endif %}
        
                
        {%- if column_boolean|length > 0 %}
         --boolean column
            {%- for boolean_v in column_boolean -%}
        ,            
        {{boolean_v}}
            {%- endfor -%}
        {%- endif %}                    
      

    from source

)

select * from renamed
{% endset %}

{% if execute %}

{{ log(base_model_sql, info=True) }}
{% do return(base_model_sql) %}

{% endif %}
{% endmacro %}  
 

      
 