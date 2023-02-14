# File takes two arguments i.e. complete file_path for crm files from different source systems.
# First : /home/kulamani/src_files/polling
# Second : crm_a.csv
# Assuming all files have same format.

# Imports
import pandas as pd
import os, io, datetime, sys
from config import connection

# Creating connection cursor
cursor = connection.stage.cursor()
stage = connection.stage
ack_file = 'knab_dwh_ack.txt'

file_path = sys.argv[1]
os.chdir(file_path)
file_name = sys.argv[2]
table_name = file_name.split('.')[0]
source = file_name.split('.')[0].split('_')[-1]

print('Started ' + file_name + ' : ', datetime.datetime.now())

# column list per file
if table_name == 'crm_a' or table_name == 'crm_b':
	src_column_list = ["customer_id", "bsn_number", "first_name", "last_name", "address", "country", "post_code", "email"]
	tgt_column_list = ["customer_id", "bsn_number", "first_name", "last_name", "address", "country", "post_code", "email",
                   "batch_identifier", "source"]
elif table_name == 'acnt':
	src_column_list = ["txn_date", "account_no", "customer_id", "account_type", "opening_balance", "closing_balance"]
	tgt_column_list = ["txn_date", "account_no", "customer_id", "account_type", "opening_balance", "closing_balance",
					   "batch_identifier", "source"]
elif table_name == 'fin_txn_a' or table_name == 'fin_txn_b':
	src_column_list = ["DateTime", "account_no", "Description", "credit_debit_ind", "txn_amount"]
	tgt_column_list = ["Date_Time", "account_no", "Description", "credit_debit_ind", "txn_amount", "batch_identifier",
					   "source"]


psql_tgt_col_list = ','.join(tgt_column_list)

# Truncate the Previous batch data
truncate_sql = 'truncate table stage_own.' + table_name
cursor.execute(truncate_sql)


def file_count_validate(ack_file, src_file):
    ack_col_list = ["file_name", "record_count"]
    df = pd.read_csv(ack_file, sep=';', header=0, names=ack_col_list, quotechar='"')
    ack_record_count = df.loc[df['file_name'] == src_file, 'record_count'].item()
    file_record_count = sum(1 for line in open(src_file + '.csv'))
    if file_record_count-1 == ack_record_count:
        return True
    else:
        print("Error !! Record Count Mismatch. File name : ", src_file + '.csv')
        print("In File : ", file_record_count-1)
        print("In ACK File : ", ack_record_count)
        return False


# Create an iterable that will read "chunksize=1000" rows
# at a time from the CSV file
if file_count_validate(ack_file, table_name):
    for df in pd.read_csv(file_name, sep=';', header=0, names=src_column_list, quotechar='"', chunksize=20000,
                          dtype='unicode'):
        output = io.StringIO()
        df['batch_identifier'] = 1
        df['source'] = source
        df.to_csv(output, sep='\t', header=True, index=False)
        output.seek(0)
        print("----------")
        copy_query = "COPY stage_own." + table_name + " (" + psql_tgt_col_list + ") " + " FROM STDOUT csv DELIMITER '\t' NULL ''  ESCAPE '\\' HEADER "
        cursor.copy_expert(copy_query, output)
        stage.commit()
    print('Finished ' + file_name + ' : ', datetime.datetime.now())
else:
    exit(1)
