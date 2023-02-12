
--Create Temporary Table for customer

drop table if exists temp_customer;
drop table if exists account_error;

CREATE TEMP TABLE temp_customer AS
(
SELECT customer_id FROM   stage_own.crm_b
union
SELECT customer_id FROM   stage_own.crm_b
);

-- Validation for Account 

CREATE TEMP TABLE account_error AS
(select batch_identifier,1 process_identifier, now(), source, 'acnt.csv' source_file_name,'account_no' primary_key_columns,account_no primary_key_columns_value, 'customer_id' error_column,customer_id error_column_value,'Referential Integrity Failed' error_description,'E' flag from stage_own.acnt
where customer_id not in
(select customer_id from temp_customer)
union all
select batch_identifier,1 process_identifier, now(), source, 'acnt.csv' source_file_name,'account_no' primary_key_columns,account_no primary_key_columns_value, 'customer_id' error_column,customer_id error_column_value,'CustomerID Can not be Null' error_description,'E' flag from stage_own.acnt
where customer_id is null
union all
select batch_identifier,1 process_identifier, now(), source, 'acnt.csv' source_file_name,'account_no' primary_key_columns,account_no primary_key_columns_value, 'customer_id' error_column,customer_id error_column_value,'CustomerID must start with CA' error_description,'E' flag from stage_own.acnt
where customer_id not like ('CA%')
union all
select batch_identifier,1 process_identifier, now(), source, 'acnt.csv' source_file_name,'account_no' primary_key_columns,account_no primary_key_columns_value, 'account_type' error_column,account_type error_column_value,'Account Type value must be (Current, Saving)' error_description,'E' flag from stage_own.acnt
where account_type not in ('Current', 'Saving')
union all
select batch_identifier,1 process_identifier, now(), source, 'acnt.csv' source_file_name,'account_no' primary_key_columns,account_no primary_key_columns_value, 'account_type' error_column,account_type error_column_value,'Account Type Can not be Null' error_description,'E' flag from stage_own.acnt
where account_type is null
union all
select batch_identifier,1 process_identifier, now(), source, 'acnt.csv' source_file_name,'account_no' primary_key_columns,account_no primary_key_columns_value, 'account_type' error_column,account_no error_column_value,'Account Number must start with KN' error_description,'E' flag from stage_own.acnt
where account_no not like ('KN%')
union all
select batch_identifier,1 process_identifier, now(), source, 'acnt.csv' source_file_name,'account_no' primary_key_columns,account_no primary_key_columns_value, 'account_type' error_column,account_no error_column_value,'Account Number Can not be Null' error_description,'E' flag from stage_own.acnt
where account_no is null
union all
select batch_identifier,1 process_identifier, now(), source, 'acnt.csv' source_file_name,'account_no' primary_key_columns,account_no primary_key_columns_value, 'opening_balance' error_column,opening_balance error_column_value,'opening_balance Can not be Text' error_description,'E' flag from stage_own.acnt
where not(opening_balance::text ~ '^\d+(\.\d+)?$')
union all
select batch_identifier,1 process_identifier, now(), source, 'acnt.csv' source_file_name,'account_no' primary_key_columns,account_no primary_key_columns_value, 'closing_balance' error_column,closing_balance error_column_value,'closing_balance Can not be Text' error_description,'E' flag from stage_own.acnt
where not(closing_balance::text ~ '^\d+(\.\d+)?$')
union all
select batch_identifier,1 process_identifier, now(), source, 'acnt.csv' source_file_name,'account_no' primary_key_columns,account_no primary_key_columns_value, 'txn_date' error_column,txn_date error_column_value,'txn_date Format is not correct' error_description,'E' flag from stage_own.acnt
where not(txn_date ~  '^\d{4}\/\d{2}\/\d{2}\ 00\:00\:00\.000$')
union all
select batch_identifier,1 process_identifier, now(), source, 'acnt.csv','account_no' source_file_name,account_no primary_key_columns_value, null error_column,null error_column_value,'Duplicate Record' error_description,'W' flag from
(SELECT batch_identifier,source,txn_date, account_no, customer_id, account_type, opening_balance, closing_balance,count(*)
  FROM stage_own.acnt
group by batch_identifier,source, txn_date, account_no, customer_id, account_type, opening_balance, closing_balance
having count(*)>1)main
);

insert into auditing_own.error_log (batch_identifier, process_identifier, process_start_time, source_system_name, 
       source_file_name, primary_key_columns, primary_key_columns_value, 
       error_column, error_column_value, error_description, flag) 
select * from account_error;

-- Load data into vlayer after all validations (Will be an improvement to be made)
insert into vlayer_own.account
select batch_identifier::int, source, txn_date::date, account_no, customer_id, 
       account_type, opening_balance::numeric, closing_balance::numeric 
from stage_own.acnt 
where account_no not in (select distinct primary_key_columns_value from account_error where flag='E');


