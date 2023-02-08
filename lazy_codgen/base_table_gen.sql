{% macro base_table_gen(source_name, table_name, partition_field=None, timezone=None, materialized=None) %}
  {{ return(adapter.dispatch('base_table_gen')(source_name, table_name,partition_field, timezone, materialized)) }}
{% endmacro %}
{% macro bigquery__base_table_gen (source_name, table_name, partition_field, timezone, materialized) %}
{# get the table relation #}
{% set source_relation = source(source_name, table_name) %}
{% set columns = adapter.get_columns_in_relation(source_relation) %}
{% set re = modules.re %}
{# List variables for different columns type #}
  {% set column_id = [] %}
  {% set column_fivetran = [] %}
  {% set column_boolean = [] %}
  {% set column_number = [] %}
  {% set column_string = [] %}
  {% set column_date = [] %}
  {% set column_datetime = [] %}
  {% set column_json = [] %}
  {% set column_others = [] %}
{# Classifying columns base on column name and data type. Currently relying on gazillion if else, need improvement #}

{% for column in columns %}
     {% if '_id' in column.name|lower or column.name == 'id'%}
      
      {% do column_id.append(column.name|lower) %}
    {% elif '_fivetran' in column.name|lower %}
      
      {% do column_fivetran.append(column.name|lower) %}

    {% elif column.dtype in ('INT64','FLOAT64','NUMERIC','BIGNUMERIC') and column_name not in column_id %}
      
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

{# Write query for model #}
{% set base_model_sql %}
             
{%- if materialized is not none -%}
    {{ "{{ config(materialized='" ~ materialized ~ "') }}" }}
{%- endif %}

with source as (
    select * from {% raw %}{{ source({% endraw %}'{{ source_name }}', '{{ table_name }}'{% raw %}) }}{% endraw %}
    {%- if partition_field is not none -%}
    {% raw %}{% if target.name == 'dev' %}{% endraw %}
    where  {{ partition_field }} >= date_sub(current_date, interval 3 day) 
    {% raw %}{% endif %}{% endraw %}
    {%-endif %}
),

renamed as (
    select
        {%- if column_id|length > 0 %} {#- Only generate query if column type exists.  -#}
        
        {%- for id in column_id -%}
        {{"," if not loop.first}}
        {{"--id_column" if loop.first -}} {#- Comment to separte columns from each other.  -#}
        {{"\n        " if loop.first -}} {#- Ensure first column in loop get proper spacing  -#}
        {{ id }}
        {%- endfor -%}
        {%- endif %}
   
   
        {%- if column_string|length > 0 %} 
            {%- for dimension in column_string -%} 
            {% if column_id|length == 0 -%} {{"," if not loop.first}} {% else -%} , {% endif %}
        {{"--string_column" if loop.first-}} 
        {{"\n        " if loop.first -}}
        {{ dimension }}
        {%- endfor -%}
        {%- endif %}

        {%- if column_date|length > 0 %}
        {%- for date in column_date  -%}
        {% if column_string+column_id |length == 0 %} {{"," if not loop.first}} {% else -%} , {% endif %}
        {{"--date_column" if loop.first -}} 
        {{"\n        " if loop.first -}}
        {{date}}
        {%- endfor -%}
        {%- endif %}   
         
        {%- if column_datetime|length > 0 %}
        {%- for datetime in column_datetime -%}
        {% if column_string+column_id+column_date|length == 0 -%} {{"," if not loop.first}} {% else -%} , {% endif %}
        {{"--timestamp_column" if loop.first -}}
        {{"\n        " if loop.first -}}
        datetime({{datetime}} {{ ", "~timezone if timezone is not none }} ) as {{ datetime }}
        {%- endfor -%}
        {%- endif %}        
        
        {%- if column_number|length > 0 %}
        {%- for fct in column_number -%}
        {% if (column_string+column_id+column_date+column_datetime)|length == 0 -%} {{"," if not loop.first}} {% else -%} , {% endif %}
        {{"--number_column" if loop.first -}} 
        {{"\n        " if loop.first -}}
        {{fct}}     
        {%- endfor -%}
        {%- endif %}
        
        {%- if column_json|length > 0 %}
            {%- for json_value in column_json -%}
        ,          
        {{"--json_column" if loop.first -}} 
        {{"\n        " if loop.first -}}
        {{ json_value }}
            {%- endfor -%}
        {%- endif %}
        
                
        {%- if column_boolean|length > 0 %}
            {%- for boolean_v in column_boolean -%}
        ,          
        {{"--boolean_column" if loop.first -}}  
        {{"\n        " if loop.first -}}
        {{boolean_v}} {%- if 'is_' not in boolean_v %} as {{'is_'~boolean_v}}  {%- endif -%}
            {%- endfor -%}
        {%- endif %}                    

        {%- if column_fivetran|length > 0 %}
         
            {%- for fivetran in column_fivetran -%}
        ,            
        {{"--fivetran_column" if loop.first -}}
        {{"\n        " if loop.first -}}
        {{fivetran}}
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
  {% set column_fivetran = []%}
  {% set column_boolean = [] %}
  {% set column_number = [] %}
  {% set column_string = [] %}
  {% set column_date = [] %}
  {% set column_datetime = [] %}
  {% set column_json = [] %}
  {% set column_others = [] %}

{% for column in columns %}
    {% if '_id' in column.name|lower or column.name == 'id'%}
      
      {% do column_id.append(column.name|lower) %}
    {% elif '_fivetran' in column.name|lower %}
      
      {% do column_fivetran.append(column.name|lower) %}
    {% elif column.dtype in ('INT','NUMBER','DECIMAL','NUMERIC','BIGNUMERIC','FLOAT','DOUBLE') and column.name|lower not in (column_id + column_fivetran)%}
      
      {% do column_number.append(column.name|lower) %}

    {% elif column.dtype in ('STRING', 'VARCHAR','CHAR','CHARACTER','TEXT') and column.name|lower not in (column_id + column_fivetran) %}

      {% do column_string.append(column.name|lower)  %}

    {% elif column.dtype in 'BOOLEAN' and column.name|lower not in (column_id + column_fivetran)%}
      
      {% do column_boolean.append(column.name|lower) %}

    {% elif column.dtype in ('DATE') and column.name|lower not in (column_id + column_fivetran) %}

      {% do column_date.append(column.name|lower) %}

     {% elif column.dtype in ('DATETIME','TIMESTAMP') and column.name|lower not in (column_id + column_fivetran)%}

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
    {%- if partition_field is not none -%}
    {% raw %}{% if target.name == 'dev' %}{% endraw %}
    where  {{ partition_field }} >= date_add(day, -3, current_date()) 
    {% raw %}{% endif %}{% endraw %}
    {%-endif %}

),



renamed as (
    select
        {%- if column_id|length > 0 %} {#- Only generate query if column type exists.  -#}
        
        {%- for id in column_id -%}
        {{"," if not loop.first}}
        {{"--id_column" if loop.first -}} {#- Comment to separte columns from each other.  -#}
        {{"\n        " if loop.first -}} {#- Ensure first column in loop get proper spacing  -#}
        {{ id }}
        {%- endfor -%}
        {%- endif %}
   
   
        {%- if column_string|length > 0 %} 
            {%- for dimension in column_string -%} 
            {% if column_id|length == 0 -%} {{"," if not loop.first}} {% else -%} , {% endif %} {#- Ensure , order are still correct when there are certain columns type missing -#}
        {{"--string_column" if loop.first-}} 
        {{"\n        " if loop.first -}}
        {{ dimension }}
        {%- endfor -%}
        {%- endif %}

        {%- if column_date|length > 0 %}
        {%- for date in column_date  -%}
        {% if (column_string+column_id) |length == 0 %} {{"," if not loop.first}} {% else -%} , {% endif %}
        {{"--date_column" if loop.first -}} 
        {{"\n        " if loop.first -}}
        {{date}}
        {%- endfor -%}
        {%- endif %}   
         
        {%- if column_datetime|length > 0 %}
        {%- for datetime in column_datetime -%}
        {% if (column_string+column_id+column_date)|length == 0 -%} {{"," if not loop.first}} {% else -%} , {% endif %}
        {{"--timestamp_column" if loop.first -}}
        {{"\n        " if loop.first -}}
        datetime({{datetime}} {{ ", "~timezone if timezone is not none }} ) as {{ datetime }}
        {%- endfor -%}
        {%- endif %}        
        
        {%- if column_number|length > 0 %}
        {%- for fct in column_number -%}
        {% if (column_string+column_id+column_date+column_datetime)|length == 0 -%} {{"," if not loop.first}} {% else -%} , {% endif %}
        {{"--number_column" if loop.first -}} 
        {{"\n        " if loop.first -}}
        {{fct}}     
        {%- endfor -%}
        {%- endif %}
        
        {%- if column_json|length > 0 %}
            {%- for json_value in column_json -%}
        ,          
        {{"--json_column" if loop.first -}} 
        {{"\n        " if loop.first -}}
        {{ json_value }}
            {%- endfor -%}
        {%- endif %}
        
                
        {%- if column_boolean|length > 0 %}
            {%- for boolean_v in column_boolean -%}
        ,          
        {{"--boolean_column" if loop.first -}}  
        {{"\n        " if loop.first -}}
        {{boolean_v}} {%- if 'is_' not in boolean_v %} as {{'is_'~boolean_v}}  {%- endif -%}
            {%- endfor -%}
        {%- endif %}                    

        {%- if column_fivetran|length > 0 %}
         
            {%- for fivetran in column_fivetran -%}
        ,            
        {{"--fivetran_column" if loop.first -}}
        {{"\n        " if loop.first -}}
        {{fivetran}}
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
 

      
 