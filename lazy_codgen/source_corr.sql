{% macro source_corr(source_name, table_name,  gen_primary_key= None,  exploratory_schema = None,  condition = None) %}
  {{ return(adapter.dispatch('source_describe')(source_name, table_name, gen_primary_key= None,  exploratory_schema = None,  condition = None)) }}
{% endmacro %}

{% macro bigquery__source_corr(source_name, table_name, gen_primary_key= None,  exploratory_schema = None,  condition = None) %}
{% set source_relation = source(source_name, table_name) %}
{% set columns = adapter.get_columns_in_relation(source_relation) %}
{% if exploratory_schema is not none %} 
    {%set exploratory_schema_value = exploratory_schema %}
{% else %} 
    {% set exploratory_schema_value = 'exploratory' %}
{% endif %}
  {% set column_boolean = [] %}
  {% set column_number = [] %}
  {% set column_string = [] %}
  {% set column_date = [] %}
  

{% for column in columns %}
    {% if column.dtype in ('INT64','FLOAT64','NUMERIC','BIGNUMERIC') %}
      
      {% do column_number.append(column.name) %}

    {% elif column.dtype in 'STRING' %}

      {% do column_string.append(column.name) %}

    {% elif column.dtype in 'BOOLEAN' %}
      
      {% do column_boolean.append(column.name) %}

    {% elif column.dtype in ('DATE','DATETIME') %}

      {% do column_date.append(column.name) %}

    {% endif %}
{% endfor %}
{% if gen_primary_key is not none %}
{% do column_string.append(gen_primary_key)%}
{% endif %}
{% if execute %}    
    {% do adapter.create_schema(api.Relation.create(database=target.database, schema= exploratory_schema_value)) %}
    {% set create_table_query %}
      
     create or replace table `{{target.project}}`.`{{exploratory_schema_value}}`.`{{table_name~'_describe'}}` as (
            with prep as (
                select *,
                
                'dummy' as partition_field
                 from {{ source_relation }}
            where 1=1 
            {%if condition is not none%}
            and {{condition}}
            {% endif %}
            
            ),
            
            calculation_cte as (
                
           
                 
            {% for query_item in ( column_string) %} 
                 select distinct
                  '{{query_item}}' as column_name,
                    COUNT( {{ query_item }}) OVER(PARTITION BY partition_field ) as count, 
                    COUNT(DISTINCT {{ query_item }}) OVER(PARTITION BY partition_field ) as count_distinct,
                    COUNTIF({{ query_item }} IS NULL) OVER(PARTITION BY partition_field) as count_null,
                    COUNTIF( lower({{ query_item }}) in ('nan','n/a','-infinty','na')) OVER(PARTITION BY partition_field) as count_na, 
                    null as average,
                    null as sum,
                    null as standard_dev,
                    null as min,
                    null as quantile_25,
                    null as quantile_50,
                    null as quantile_75,
                    null as quantile_90,
                    null as max
                 from prep 
                {{"union all " if not loop.last}}
               
                {% endfor %} 
            {% for query_item in ( column_date + column_boolean) %} 
                 union all
                 select distinct
                  '{{query_item}}' as column_name,
                    COUNT( {{ query_item }}) OVER(PARTITION BY partition_field ) as count, 
                    COUNT(DISTINCT {{ query_item }}) OVER(PARTITION BY partition_field ) as count_distinct,
                    COUNTIF({{ query_item }} IS NULL) OVER(PARTITION BY partition_field) as count_null,
                    null as count_na, 
                    null as average,
                    null as sum,
                    null as standard_dev,
                    null as min,
                    null as quantile_25,
                    null as quantile_50,
                    null as quantile_75,
                    null as quantile_90,
                    null as max
                 from prep 
                {{"union all " if not loop.last}}
               
            {% endfor %} 
            {% for query_item in column_number %}
                    union all 
                    select distinct
                   '{{query_item}}' as column_name,
                    COUNT( {{ query_item }}) OVER(PARTITION BY partition_field ) as count, 
                    COUNT(DISTINCT {{ query_item }}) OVER(PARTITION BY partition_field ) as  count_distinct,
                    COUNTIF({{ query_item }} IS NULL) OVER(PARTITION BY partition_field) as count_null,
                    null as count_na, 
                    AVG(COALESCE ({{ query_item }}, 0 )) OVER(PARTITION BY partition_field ) as average,
                    SUM(COALESCE ({{ query_item }}, 0 )) OVER(PARTITION BY partition_field ) as sum,
                    STDDEV(COALESCE ({{ query_item }}, 0 )) OVER(PARTITION BY partition_field ) as  standard_dev,
                    MIN(COALESCE ({{ query_item }}, 0 )) OVER (PARTITION BY partition_field)  as min,
                    PERCENTILE_CONT ({{ query_item }},0.25) OVER(PARTITION BY partition_field ) as quantile_25,
                    PERCENTILE_CONT ({{ query_item }},0.5) OVER(PARTITION BY partition_field ) as quantile_50,
                    PERCENTILE_CONT ({{ query_item }},0.75) OVER(PARTITION BY partition_field )  as quantile_75,
                    PERCENTILE_CONT ({{ query_item }},0.9) OVER(PARTITION BY partition_field )  as quantile_90,
                    MAX (COALESCE ({{ query_item }}, 0 )) OVER (PARTITION BY partition_field)  as max
                  from prep 
                {% endfor %} 
        
            )
           select  * from calculation_cte
            

    

      
 );
             
    {% endset %}

  {% do run_query(create_table_query) %}
  {{ log('Create describe table ' ~  table_name ~ ' in ' ~ exploratory_schema_value, info=True) }}


{% endif %}
{% endmacro %}

{% macro snowflake__source_corr(source_name, table_name, gen_primary_key= None,  exploratory_schema = None,  condition = None) %}
{% set source_relation = source(source_name, table_name) %}
{% set columns = adapter.get_columns_in_relation(source_relation) %}
{% if exploratory_schema is not none %} 
    {%set exploratory_schema_value = exploratory_schema %}
{% else %} 
    {% set exploratory_schema_value = 'exploratory' %}
{% endif %}
  {% set column_boolean = [] %}
  {% set column_number = [] %}
  {% set column_string = [] %}
  {% set column_date = [] %}
  

{% for column in columns %}
    {% if column.dtype in ('INT','NUMBER','DECIMAL','NUMERIC','BIGNUMERIC','FLOAT','DOUBLE') %}
      
      {% do column_number.append(column.name) %}

    {% elif column.dtype in ('STRING', 'VARCHAR','CHAR','CHARACTER','TEXT') %}

      {% do column_string.append(column.name) %}

    {% elif column.dtype in 'BOOLEAN' %}
      
      {% do column_boolean.append(column.name) %}

    {% elif column.dtype in ('DATE','DATETIME','TIME,TIMESTAMP') %}

      {% do column_date.append(column.name) %}

    {% endif %}
{% endfor %}
{% if gen_primary_key is not null %}
{% do column_string.append(gen_primary_key)%}
{% endif %}
    {% if execute %}    
        {% do adapter.create_schema(api.Relation.create(database=target.database, schema= exploratory_schema_value)) %}
        {% set create_table_query %}
          
         create or replace transient table {{target.project}}.{{exploratory_schema_value}}.{{table_name~'describe'}} as (
                with prep as (
                    select *
                     from {{ source_relation }}
                where 1=1 
                {%if condition is not none%}
                and {{condition}}
                {% endif %}
                
                ),
                
                calculation_cte as (
                    
            
                     
                {% for query_item in ( column_string) %} 
                     select 
                       '{{query_item}}' as column_name,
                        COUNT( {{ query_item }}) as count, 
                        COUNT(DISTINCT {{ query_item }}) as count_distinct,
                        COUNT_IF({{ query_item }} IS NULL)  as count_null,
                        COUNT_IF(lower({{ query_item }}) in ('nan','n/a','-infinty','na')) as count_na, 
                        null as average,
                        null as sum,
                        null as standard_dev,
                        null as min,
                        null as quantile_25,
                        null as quantile_50,
                        null as quantile_75,
                        null as quantile_90,
                        null as max
                     from prep 
                     group by 1
                    {{"union all " if not loop.last}}
                   
                    {% endfor %} 
                {% for query_item in ( column_date + column_boolean) %} 
                     select
                       '{{query_item}}' as column_name,
                        COUNT( {{ query_item }}) as count, 
                        COUNT(DISTINCT {{ query_item }}) as count_distinct,
                        COUNT_IF({{ query_item }} IS NULL)  as count_null,
                        COUNT_IF(lower({{ query_item }}) in ('nan','n/a','-infinty','na')) as count_na, 
                        null as average,
                        null as sum,
                        null as standard_dev,
                        null as min,
                        null as quantile_25,
                        null as quantile_50,
                        null as quantile_75,
                        null as quantile_90,
                        null as max
                     from prep 
                    group by 1
                    {{"union all " if not loop.last}}
                   
                {% endfor %} 
            
                {% for query_item in column_number %}
                        union all 
                        select 
                       '{{query_item}}' as column_name,
                        COUNT( {{ query_item }})  as count, 
                        COUNT(DISTINCT {{ query_item }}) as  count_distinct,
                        COUNT_IF({{ query_item }} IS NULL)  as count_null,
                        null as count_na, 
                        AVG(COALESCE ({{ query_item }}, 0 ))  as average,
                        SUM(COALESCE ({{ query_item }}, 0 )) as sum,
                        STDDEV(COALESCE ({{ query_item }}, 0 )) as  standard_dev,
                        MIN(COALESCE ({{ query_item }}, 0 ))  as min,
                        PERCENTILE_CONT ({{ query_item }},0.25)  as quantile_25,
                        PERCENTILE_CONT ({{ query_item }},0.5)  as quantile_50,
                        PERCENTILE_CONT ({{ query_item }},0.75) as quantile_75,
                        PERCENTILE_CONT ({{ query_item }},0.9)   as quantile_90,
                        MAX (COALESCE ({{ query_item }}, 0 )) as max
                      from prep 
                       group by 1
                    {% endfor %} 
            
                )
               select  * from calculation_cte
                
    
        
    
          
     );
                 
        {% endset %}
    
    {% do run_query(create_table_query) %}
    {{ log('Create describe table ' ~  table_name ~ ' in ' ~ exploratory_schema_value, info=True) }}
    {% set check_null_query %}
             select column_name from {{target.project}}.{{exploratory_schema_value}}.{{table_name~'describe'}}
             where count_null > 0
    {% endset %}
    


{% endif %}
{% endmacro %}

