import yaml
import subprocess
from platform import system


def get_base_tables_and_source(file_path, source_index):
	file = open(file_path)
	sources = yaml.load(file, Loader=yaml.FullLoader)
	tables_configs = sources['sources'][source_index]['tables']
	table_names = [item['name'] for item in tables_configs]
	source_name = sources['sources'][source_index]['name']
	return table_names, source_name

def generate_base_model(table_name, macro_name, source_name):
	print(f'Generating base model for table {table_name}')
	bash_command = f'''
		dbt run-operation  {macro_name} --args \'{{"source_name": "{source_name}", "table_name": "{table_name}"}}\'
	'''
	if system() == 'Windows':
	    output = subprocess.check_output(["powershell.exe",bash_command]).decode("utf-8")
	else:
		output = subprocess.check_output(bash_command, shell=True).decode("utf-8")
	sql_index = output.lower().find('with source as')
	sql_query = output[sql_index:]
	return sql_query

def generate_yml(source):
	print(f'Generating yml file for "{source}" ')
	bash_command = f'''
		dbt run-operation generate_source --args \'{{"schema_name": "{source}"}}\'
	'''
	if system() == 'Windows':
	    output = subprocess.check_output(["powershell.exe",bash_command]).decode("utf-8")
	else:
		output = subprocess.check_output(bash_command, shell=True).decode("utf-8")
	ymlfile = output.lower().find('version:')
	yml_result = output[ymlfile:]
	return yml_result

def fixsql(output_path):
	print(f'Linting generated files ')
	bash_command = f'''
		sqlfluff fix  {output_path} -f
	'''
	if system() == 'Windows':
	    output = subprocess.check_output(["powershell.exe",bash_command]).decode("utf-8")
	else:
		output = subprocess.check_output(bash_command, shell=True).decode("utf-8")
	print ('Linting completed')
