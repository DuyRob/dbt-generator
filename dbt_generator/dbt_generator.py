import os
import click
from pathlib import Path
from .generate_base_models import *
from .process_base_models import *
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
@click.option('-mc','--macro-name', type=str, default='generate_base_model' , help='select macro to be used')
@click.option('-o', '--output-path', type=click.Path(), help='Path to write generated models')
@click.option('-m', '--model', type=str, default='', help='Select one model to generate')
@click.option('-c', '--custom_prefix', type=str, default='', help='Enter a Custom String Prefix for Model Filename')
@click.option('--model-prefix', type=bool, default=False, help='Prefix model name with source_name + _')
@click.option ('-d','--describe', is_flag = True, help='Describe table aftergenerating them')
@click.option ('-l','--linting', is_flag = True, help='Describe table aftergenerating them')
@click.option('--source-index', type=int, default=0, help='Index of the source to generate base models for')
def genbase(source_yml, macro_name, output_path, source_index, model, describe, linting,custom_prefix, model_prefix):
    tables, source_name = get_base_tables_and_source(source_yml, source_index)
    if model:
        tables = [model]
    for table in tables:
        file_name = custom_prefix + table + '.sql'
        if model_prefix:
            file_name = source_name + '_' + file_name
        if describe:
            describe_table( source_name, table, 'exploratory')
        query = generate_base_model(table, macro_name, source_name)
        file = open(os.path.join(output_path, file_name), 'w', newline='')
        file.write(query)
        if linting:
            fixsql(output_path)

@dbt_generator.command(help='Describe the table')
@click.option('-t', '--table', type=str, help='Source .yml file to be used')
@click.option('-s','--schema', type=str, default='generate_base_model', help='select macro to be used')
@click.option('-o', '--output', type=str, default='exploratory', help='schema to save the results to')
@click.option('-c', '--condition', type=str, default=None, help='schema to save the results to')
def describe (table, schema, output, condition):
    describe_table( schema, table,output, condition)
    print( 'Describe table generated')
@dbt_generator.command(help='Calculate correlation' )
@click.option('-t', '--table',type=str, help='table to be generated')
@click.option('-s','--schema',type=str, help='select macro to be used')
@click.option('-o', '--output',type=str, default='exploratory', help='schema to save the results to')
@click.option('-c', '--condition',type=str, default=None, help='schema to save the results to')
def correlation(table, schema, output, condition):
        
    corr( schema, table, output, condition)
    print( 'Correlation table generated')

@dbt_generator.command(help='Transform one base model using a transforms.yml file')
@click.option('-m', '--model-path', type=click.Path(), help='The path to one single model')
@click.option('-t', '--transforms-path', type=click.Path(), help='Path to a .yml file containing transformations')
@click.option('-o', '--output-path', type=click.Path(), help='Path to write transformed models to')
@click.option('--drop-metadata', type=bool, help='Toptionally drop source columns prefixed with "_" if that designates metadata columns not needed in target', default=True)
@click.option('--case-sensitive', type=bool, help='(default=False) treat column names as case-sensitive - otherwise force all to lower', default=False)
def transforms(model_path, transforms_path, output_path, drop_metadata, case_sensitive):
    file_name = get_file_name(model_path)
    processor = ProcessBaseModelsWithTransforms(
        model_path, transforms_path, drop_metadata, case_sensitive)
    processor.process_base_models(os.path.join(output_path, file_name))


@dbt_generator.command(help='Transform base models in a directory for BigQuery source')
@click.option('-m', '--model-path', type=click.Path(), help='The path to models')
@click.option('-o', '--output-path', type=click.Path(), help='Path to write transformed models to')
@click.option('--drop-metadata', type=bool, help='Toptionally drop source columns prefixed with "_" if that designates metadata columns not needed in target', default=True)
@click.option('--case-sensitive', type=bool, help='(default=False) treat column names as case-sensitive - otherwise force all to lower', default=False)
@click.option('--split-columns', type=bool, help='Split column names. E.g. currencycode => currency_code', default=False)
@click.option('--id-as-int', type=bool, help='Convert id to int', default=False)
@click.option('--convert-timestamp', type=bool, help='Convert timestamp to datetime', default=False)
def bq_transform(model_path, output_path, drop_metadata, case_sensitive, split_columns, id_as_int, convert_timestamp):
    sql_files = get_sql_files(model_path)
    for sql_file in sql_files:
        processor = ProcessBaseModelsBQ(os.path.join(
            model_path, sql_file), drop_metadata, case_sensitive, split_columns, id_as_int, convert_timestamp)
        processor.process_base_models(os.path.join(output_path, sql_file))


@dbt_generator.command(help='Transform base models in a directory for Snowflake source')
@click.option('-m', '--model-path', type=click.Path(), help='The path to models')
@click.option('-o', '--output-path', type=click.Path(), help='Path to write transformed models to')
@click.option('--drop-metadata', type=bool, help='Toptionally drop source columns prefixed with "_" if that designates metadata columns not needed in target', default=True)
@click.option('--case-sensitive', type=bool, help='(default=False) treat column names as case-sensitive - otherwise force all to lower', default=False)
@click.option('--split-columns', type=bool, help='Split column names. E.g. currencycode => currency_code', default=False)
@click.option('--id-as-int', type=bool, help='Convert id to int', default=False)
@click.option('--convert-timestamp', type=bool, help='Convert timestamp to datetime', default=False)
def sf_transform(model_path, output_path, drop_metadata, case_sensitive, split_columns, id_as_int, convert_timestamp):
    sql_files = get_sql_files(model_path)
    for sql_file in sql_files:
        processor = ProcessBaseModelsSF(os.path.join(
            model_path, sql_file), drop_metadata, case_sensitive, split_columns, id_as_int, convert_timestamp)
        processor.process_base_models(os.path.join(output_path, sql_file))


if __name__ == '__main__':
    dbt_generator()

