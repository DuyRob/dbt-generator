import os
import click
from pathlib import Path
from .generate_base_models import *
from .explore import*



def get_file_name(file_path):
    return os.path.basename(file_path)

@click.group(help='Generate and process base dbt models')

def dbt_generator():
    pass


@dbt_generator.command(help='Generate base yml files')
@click.option('-s','--source', type=str, help='name of schema to generate')
@click.option('-o','--output', type=click.Path(), help='output of base code file')
@click.option('-y','--yml_prefix', type=str, default='', help='prefix for yml file')
def ymlgen(source, output, yml_prefix):
    ymlfile = generate_yml(source)  
    ymlname = yml_prefix + '_' + source +'.yml'
    ymlpath = Path(os.path.join(output, ymlname))
    file_yml = open(ymlpath,'w',newline='')
    file_yml.write(ymlfile)
    print( 'Yml file generated')
    
@dbt_generator.command(help='Gennerate base models based on a .yml source')
@click.option('-s', '--source-yml', type=click.Path(), help='Source .yml file to be used')
@click.option('-o', '--output-path', type=click.Path(), help='Path to write generated models')
@click.option('-m', '--model', type=str, default='', help='Select one model to generate')
@click.option('-t', '--timezone', type=str, default=None, help='Convert all detected timestamp column to a timezone')
@click.option('-c', '--custom-prefix', type=str, default='', help='Enter a Custom String Prefix for Model Filename')
@click.option('--model-prefix', type=bool, default=False, help='Prefix model name with source_name + _')
@click.option ('-d','--describe', is_flag = True, help='Describe table after generating them')
@click.option ('-cr','--correlation', is_flag = True, help='Calculate table correlation after generating them')
@click.option ('-dc','--describe-condition', default=None, help='Describe query condition')
@click.option ('-l','--linting', is_flag = True, help='Linting the table after generating them')
@click.option('--source-index', type=int, default=0, help='Index of the source to generate base models for')
def generate(source_yml,  output_path, source_index, timezone, model, describe, correlation, describe_condition, linting,custom_prefix, model_prefix):
    tables, source_name = get_base_tables_and_source(source_yml, source_index)
    if model:
        tables = [model]
    for table in tables:
        file_name = custom_prefix + table + '.sql'
        if model_prefix:
            file_name = source_name + '_' + file_name
        if describe:
            describe_table( source_name, table, 'exploratory',describe_condition)
        if correlation:
            corr(source_name, table, 'exploratory',describe_condition)
        query = generate_base_model(table, source_name, timezone)
        file = open(os.path.join(output_path, file_name), 'w', newline='')
        file.write(query)
        if linting:
            fixsql(output_path)

@dbt_generator.command(help='Describe the table')
@click.option('-t', '--table', type=str, help='Source .yml file to be used')
@click.option('-s','--schema', type=str, default='generate_base_model', help='table schema/dataset ')
@click.option('-o', '--output', type=str, default='exploratory', help='schema to save the results to')
@click.option('-c', '--condition', type=str, default=None, help='Query condition to save cost. Might introduce skews')
def describe (table, schema, output, condition):
    describe_table( schema, table,output, condition)
    print( 'Describe table generated')

@dbt_generator.command(help='Calculate correlation' )
@click.option('-t', '--table',type=str, help='table to calculate correlation over')
@click.option('-s','--schema',type=str, help='table schema/dataset ')
@click.option('-o', '--output',type=str, default='exploratory', help='schema to save the results to')
@click.option('-c', '--condition',type=str, default=None, help='Query condition to save cost. Might introduce skews.')
def correlation(table, schema, output, condition):
        
    corr( schema, table, output, condition)
    print( 'Correlation table generated')


if __name__ == '__main__':
    dbt_generator()

