from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

from geotab_operators.airflow_lineage_operators import DagRunTaskInstanceRelationshipOperator

default_args = {
		'owner': 'airflow',
		'depends_on_past': False,
		'start_date': datetime(2018, 4, 5),
		'email': ['bi-jobs@geotab.com'],
		'email_on_failure': True,
		'email_on_retry': False,
		'retries': 3,
		'retry_delay': timedelta(minutes=5),
		# 'queue': 'bash_queue',
		# 'pool': 'backfill',
		# 'priority_weight': 10,
		# 'end_date': datetime(2016, 1, 1),
}

CONNECTION_ID = 'DATABASE_CONN_ID'

def clean_dict(dict_string):
	"""Strips the leading dashes from the dictionary text and converts the string to a dictionary object

						dict_string: first line of the SQL file which contains a dictionary object									
	returns: A dictionary object
	"""
	new_dict = dict_string.replace('--','').replace('\n','')
	try:
		dict = eval(new_dict)
	except:
		dict = None

	return dict
		
# Overwrite any default arguments from the sql file
def apply_defaults(default_dict, sql_dict):
	"""Override key/value pairs in the DAG default settings
			
			Given a default dictionary and a second dictionary from the SQL file ovveride any default values.				

						default_dict: The default dictionary for the DAG.	
						sql_dict: The dictionary included in the top of the SQL file which may contain default values
	returns: A dictionary object
	"""	
	replace_values = {key:value for (key,value) in sql_dict.items() if key in default_dict.keys()}

	new_defaults = default_args.copy()

	for k, v in replace_values.items():
		new_defaults[k] = v

	return new_defaults
	

def create_dag(default_args, sql_dict, file_name, index):
	"""Override key/value pairs in the DAG default settings
			
			Given a default dictionary and a second dictionary from the SQL file ovveride any default values.				
	
						default_dict: The default dictionary for the DAG.	
						sql_dict: The dictionary included in the top of the SQL file which may contain default values
	returns: A dictionary object
	"""
	schedule_template = '{} {} * * *'
	hour = 7
	minute = 0
	interval = 2
	hour = hour + index // 60 
	minute = minute + ((index*interval)%60)
  DEFAULT_VERSION = 1
	
	default_arguments = apply_defaults(default_args, sql_dict)
	
	# check for a custom schedule
	if(sql_dict['schedule_interval']):
		schedule = sql_dict['schedule_interval']
	else:
		schedule = schedule_template.format(minute, hour)
	
	maxActiveRuns = 10
	# check for a max_active_runs, by default this will be set to 10
	if('max_active_runs' in sql_dict):
		maxActiveRuns = sql_dict['max_active_runs']


	catchup = False
	# check for catchup=True, by default this is false
	if('catchup' in sql_dict.keys()):
		catchup = sql_dict['catchup']
    
    if 'version' in sql_dict.keys(): 
        dag_name = 'etl_dags_v' + str(sql_dict['version']) + '_' + file_name.replace('.sql','') 
    else: 
        dag_name = 'etl_dags_v' + DEFAULT_VERSION + '_' + file_name.replace('.sql','')
	
	if('destination_table' in sql_dict):
		destination_table = sql_dict['destination_table']

	checkQuery = '''
					SELECT count(1) as Num FROM	[{}]
					'''.format(destination_table)	

	if('check_query' in sql_dict):
		checkQuery = sql_dict['check_query']
	
	
	with DAG(dag_name, schedule_interval=schedule, max_active_runs=maxActiveRuns, catchup=catchup, default_args=default_arguments) as dag:
		etl_task = BigQueryOperator(
					task_id = dag_name + '_etl',
					bql = 'ETL/' + file_name,
					destination_dataset_table = sql_dict['destination_table'],
					write_disposition = 'WRITE_TRUNCATE',
					bigquery_conn_id = CONNECTION_ID,
					use_legacy_sql = False)

		check = BigQueryCheckOperator(
					bigquery_conn_id = CONNECTION_ID,
					task_id= dag_name + '_etl_check',
					sql=checkQuery)
		export_lineage_to_metadatadb = DagRunTaskInstanceRelationshipOperator(task_id='export_lineage_to_metadatadb', gcs_connect_id=CONNECTION_ID, gbq_connect_id=CONNECTION_ID)

		etl_task >> check >> export_lineage_to_metadatadb

		globals()[dag_name] = dag

def find_etls():
	"""Iterate over the SQL file in the ETL folder and capture their settings dictionaries		

	returns: A list of ETL dag names and their settings
	"""
	from os import listdir
	from os.path import isfile, join, abspath
	folderPath = abspath("/etc/airflow/dags/ETL")
	onlyfiles = [f for f in listdir(folderPath) if isfile(join(folderPath, f))]
	etl_dags = []

	for file in onlyfiles:			
		with open(folderPath+'/'+file, 'r') as f:						
			first_line = f.readline()
			if(first_line.startswith('--')):
				new_dictionary = clean_dict(first_line)
				if not new_dictionary is None:
					etl_dags.append((new_dictionary,file))		
	return etl_dags

def create_etl_dags():		
	index=0
	etls_to_create = find_etls()
	for etl_item in etls_to_create:
		create_dag(default_args, etl_item[0], etl_item[1], index)
		index=index+1		

create_etl_dags()
