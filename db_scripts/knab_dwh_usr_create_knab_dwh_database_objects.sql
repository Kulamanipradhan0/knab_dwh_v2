\echo Executing DB Objects

\i /code_base/db_scripts/create_schema.sql


\echo Creating auditing_own objects
\i /code_base/db_scripts/auditing/auditing_own_batch_information.sql
\i /code_base/db_scripts/auditing/auditing_own_process_information.sql
\i /code_base/db_scripts/auditing/auditing_own_error_log.sql

\echo Creating stage_own objects
\i /code_base/db_scripts/staging/stage_own_crm_a.sql
\i /code_base/db_scripts/staging/stage_own_crm_b.sql
\i /code_base/db_scripts/staging/stage_own_fin_txn_a.sql
\i /code_base/db_scripts/staging/stage_own_fin_txn_b.sql
\i /code_base/db_scripts/staging/stage_own_acnt.sql
--\i /code_base/db_scripts/staging/stage_own_xref_cust_acnt.sql


\echo Partition Function

\i /code_base/db_scripts/dwh/fn_create_yearmonth_auto_parttables.sql
\i /code_base/db_scripts/dwh/fn_create_yearmonth_auto_partition.sql


\echo Creating dwh_own objects
\i /code_base/db_scripts/dwh/dwh_own_dim_calendar_date.sql
\i /code_base/db_scripts/dwh/dwh_own_dim_calendar_time.sql
\i /code_base/db_scripts/dwh/dwh_own_dim_customer.sql
\i /code_base/db_scripts/dwh/dwh_own_dim_account.sql
\i /code_base/db_scripts/dwh/dwh_own_xref_account_to_customer.sql

\i /code_base/db_scripts/dwh/dwh_own_fac_fin_transaction.sql
\i /code_base/db_scripts/dwh/dwh_own_fac_acc_balance.sql
\i /code_base/db_scripts/dwh/dwh_own_fac_dwh_eod_position.sql
