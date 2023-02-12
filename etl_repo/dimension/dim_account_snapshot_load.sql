/* Implementation for dim_account */
/* Expected source to deliver all active accounts with opening & closing balance everyday */
/* If any account is missing, we will mark it as inactive in our system */
/* Ideally account would be a SCD Type-2 load. As I have only one attribute i.e. account_type hence not implemented */

BEGIN;

drop table IF EXISTS account;
drop table IF EXISTS active_dim_account;

--Create Temporary Table for account

CREATE TEMP TABLE account AS
(
SELECT distinct account_no,
                account_type,
                batch_identifier
FROM   stage_own.acnt
);

--Create Temporary Table for active dim_account

CREATE TEMP TABLE active_dim_account AS
(
SELECT account_no,
       account_type,
       account_key 
FROM   dwh_own.dim_account
WHERE  active_flag = 'Y');

-- New account Load to Dimension
with 
src as(select * from account)  ,
tgt as (select * from active_dim_account)
insert into dwh_own.dim_account (account_no,account_type,effective_start_date, insert_datetime, last_update_datetime,batch_identifier)
select 
src.account_no,
src.account_type,
To_char(CURRENT_TIMESTAMP, 'yyyy-mm-dd') :: date AS start_date,
To_char(CURRENT_TIMESTAMP, 'yyyy-mm-dd hh:mi:ss.ms') ::  timestamp AS insert_datetime,
To_char(CURRENT_TIMESTAMP, 'yyyy-mm-dd hh:mi:ss.ms') ::  timestamp AS last_update_datetime,
batch_identifier
from 
src left join tgt on
src.account_no=tgt.account_no
where tgt.account_no is null --New accounts
; 


-- Update Inactive accounts to N in DWH
update dwh_own.dim_account tgt
set last_update_datetime=To_char(CURRENT_TIMESTAMP, 'yyyy-mm-dd hh:mi:ss.ms')::  timestamp, active_flag='N', effective_end_date=To_char(CURRENT_TIMESTAMP, 'yyyy-mm-dd') :: date 
from 
account src 
where 
tgt.account_no not in (select distinct src.account_no from account src)
and tgt.active_flag='Y';


END;
