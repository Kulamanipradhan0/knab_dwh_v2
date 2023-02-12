#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Example DAG demonstrating the usage of the BashOperator."""
import os

repo_home_dir='/code_base'

from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
import psycopg2 as pg

pg_port=9000
stage = pg.connect(host='localhost',
                    port=pg_port,
                    database='postgres',
                    user='postgres',
                    password='password')
print("You are connected to Stage database(User : postgres)")
cursor = stage.cursor()


args = {
    'owner': 'airflow',
}

business_date='2020-12-31'
business_date_int=str(20201231)

def process_start(business_date,process_name,current_batch_id):
    current_process_id_query = 'select coalesce(max(process_identifier),0)+1 process_identifier from auditing_own.process_information '
    cursor.execute(current_process_id_query)
    current_process_id = str(cursor.fetchone()[0])
    current_batch_id=str(current_batch_id)

    process_open_query='''INSERT INTO auditing_own.process_information(process_identifier,batch_identifier, process_name, process_start_time, 
            process_end_time, process_status, process_status_description, source_system_name, source_file_name)
            values
            ('''+current_process_id+','+current_batch_id+','+"'"+process_name+"'"+''',now() ,NULL,'S', 'Started', '', '${inp_file_name}')'''

    cursor.execute(process_open_query)
    print('Process Started for ' + process_name)
    stage.commit()
    return current_process_id

def process_end(business_date,process_name,process_id,current_batch_id,flag):
    current_batch_id=str(current_batch_id)
    process_id=str(process_id)
    if flag=='C':
        process_status_description='Completed'
    elif flag=='F':
        process_status_description = 'Failed'

    process_close_query='''update auditing_own.process_information
            set process_end_time=now(), process_status='C', process_status_description='''+"'"+process_status_description+"'"+'''
            where batch_identifier='''+current_batch_id+'''
            and process_identifier='''+process_id+';'
    cursor.execute(process_close_query)
    print('Process Done for ' + process_name)
    stage.commit()


with DAG(
    dag_id='knab_dwh_stg',
    default_args=args,
    schedule_interval='0 0 * * *',
    start_date=days_ago(2),
    dagrun_timeout=timedelta(minutes=240),
    tags=['olap', 'knab_dwh'],
    params={"example_key": "example_value"},
) as dag:

    task_id='batch_start'
    command='cd '+repo_home_dir+'/etl_repo/staging/config;'+\
            'python3 -c '+"'"+'import auditing ; auditing.'+task_id+'('+str(business_date_int)+')'+"'"
    run_batch_start = BashOperator(
        task_id=task_id,
        bash_command=command,
    )
    #Find Batch ID
    current_batch_id_query = "select max(batch_identifier) from auditing_own.batch_information where batch_end_time is null and batch_Status in ('Failed','Started') and business_date=" + str(business_date_int)
    cursor.execute(current_batch_id_query)
    current_batch_id = str(cursor.fetchone()[0])

    #Staging Batch Loading start
    os.chdir(repo_home_dir)
    task_id = 'crm_a_stg_load'
    stg_job_name='stg_load.py'
    file_name='crm_a.csv'
    stg_command='python3 '+repo_home_dir+'/etl_repo/staging/'+stg_job_name+' ' +repo_home_dir+'/src_files/polling '
    command = stg_command+file_name
    run_crm_a_stg = BashOperator(
            task_id=task_id,
            bash_command=command,
        )

    task_id = 'crm_b_stg_load'
    file_name = 'crm_b.csv'
    command = stg_command+file_name
    run_crm_b_stg = BashOperator(
        task_id=task_id,
        bash_command=command,
    )

    task_id = 'acnt_stg_load'
    stg_job_name='stg_load.py'
    file_name = 'acnt.csv'
    command = stg_command + file_name
    run_acnt_stg = BashOperator(
        task_id=task_id,
        bash_command=command,
    )


    task_id = 'fin_txn_a_stg_load'
    file_name = 'fin_txn_a.csv'
    command = stg_command + file_name
    run_fin_txn_a_stg = BashOperator(
        task_id=task_id,
        bash_command=command,
    )

    task_id = 'fin_txn_b_stg_load'
    file_name = 'fin_txn_b.csv'
    command = stg_command + file_name
    run_fin_txn_b_stg = BashOperator(
        task_id=task_id,
        bash_command=command,
    )

    #Dummy Job
    task_id = 'run_staging_done'
    command = 'ls'
    run_staging_done = BashOperator(
        task_id=task_id,
        bash_command=command,
    )

    task_id = 'batch_end'
    command = 'cd ' + repo_home_dir + '/etl_repo/staging/config;' + \
              'python3 -c ' + "'" + 'import auditing ; auditing.' + task_id + '(' + business_date_int + ', '+current_batch_id+')' + "'"
    run_batch_end = BashOperator(
        task_id=task_id,
        bash_command=command,
    )
    #JobFlow set up
    run_batch_start >> [run_crm_a_stg, run_crm_b_stg, run_fin_txn_a_stg, run_fin_txn_b_stg, run_acnt_stg] >> run_staging_done
    run_staging_done >> run_batch_end
if __name__ == "__main__":
    dag.cli()
