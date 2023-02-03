import subprocess
from platform import system

def generate_yml_filepath(source, output, yml_prefix):
	filepath = os.path.join(output, yml_prefix + source +'.yml')
	return filepath

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
