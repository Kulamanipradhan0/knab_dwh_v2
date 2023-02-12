import connection, sys

cursor = connection.stage.cursor()
stage=connection.stage


def batch_start(business_date):
    current_batch_id_query = 'select coalesce(max(batch_identifier),0)+1 from auditing_own.batch_information'
    cursor.execute(current_batch_id_query)
    current_batch_id = str(cursor.fetchone()[0])
    business_date=str(business_date)
    knab_dwh_batch_open_query='''INSERT INTO auditing_own.batch_information(
            batch_identifier, batch_name, environment, business_date, batch_version, 
            batch_success_flag, batch_start_time, batch_end_time, batch_status, 
            monthly_batch_flag)
            VALUES ('''+current_batch_id+''', 'KNAB_DWH', 'PROD', '''+business_date+''', 
            (select coalesce(max(batch_version),0)+1 from auditing_own.batch_information where batch_identifier='''+current_batch_id+' and business_date= '+business_date+'''), 
            'S', now(), null, 'Started', 
            'N');'''
    cursor.execute(knab_dwh_batch_open_query)
    stage.commit()
    print('Batch Start for '+business_date+'. Batch ID : '+current_batch_id)
    return current_batch_id

def batch_end(business_date,current_batch_id):
    business_date=str(business_date)
    current_batch_id=str(current_batch_id)
    knab_dwh_batch_close_query = '''update auditing_own.batch_information
            set batch_end_time=now(), batch_status='Completed', batch_success_flag='Y'
            where business_date='''+business_date+' and batch_identifier='+current_batch_id+' and batch_end_time is null;'
    cursor.execute(knab_dwh_batch_close_query)
    print('Batch Done for ' + business_date + '. Batch ID : ' + current_batch_id)
    stage.commit()


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

