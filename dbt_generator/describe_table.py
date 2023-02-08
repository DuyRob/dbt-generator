import subprocess
from platform import system

def describe_table(source,table,output):
	print(f'Generating exploratory table for "{table}" in "{output}" ')
	bash_command = f'''
		dbt run-operation source_describe --args \'{{"source_name": "{source}", "table_name":"{table}", "exploratory_schema":"{output}"}}\'
	'''
	if system() == 'Windows':
	    output = subprocess.check_output(["powershell.exe",bash_command]).decode("utf-8")
	else:
		output = subprocess.check_output(bash_command, shell=True).decode("utf-8")
	log = output
	print(log)
	return log
