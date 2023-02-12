BEGIN ; 

--Used only when we want to rerun for same batch
delete from dwh_own.fac_fin_transaction
where batch_identifier=(select distinct batch_identifier from stage_own.fin_txn_a);

--Drop temporary tables (Needed only for testing purpose)
drop table if exists fin_transaction_src;
drop table if exists fin_transaction;
drop table if exists customer;

-- Create Temporary Table for fin_txn
CREATE TEMP TABLE fin_transaction AS (
select date_time as txn_datetime,
	account_no,	
	credit_debit_ind,
	txn_amount,
        batch_identifier
  from stage_own.fin_txn_a t
union 
select date_time as txn_datetime,
	account_no,	
	credit_debit_ind,
	txn_amount,
        batch_identifier
  from stage_own.fin_txn_b t);

--Create Temporary Table for customer

CREATE TEMP TABLE customer AS
(
SELECT customer_id
FROM   stage_own.crm_a 
union
SELECT customer_id
FROM   stage_own.crm_b
);

-- Joing Stage tables to get customerid from account table

CREATE TEMP TABLE fin_transaction_src AS
(select t.account_no, c.customer_id,t.txn_datetime,credit_debit_ind, txn_amount, a.opening_balance, t.batch_identifier from fin_transaction t 
join stage_own.acnt a on t.account_no=a.account_no
join customer c on c.customer_id=a.customer_id );

--Loading fact tables with grain as Account, Customer, Txn time
insert into dwh_own.fac_fin_transaction
(customer_key, account_key, txn_date, txn_time_hhmm, txn_datetime_full, credited_amount, debited_amount, current_balance, batch_identifier, insert_datetime)
select customer_key,account_key, date_key , timeofday,txn_datetime, credited_amount, debited_amount,
--Calculating current balance per account and txn time(ideally it should be on txn reference no)
opening_balance+coalesce(sum(txn_amount) OVER (partition by account_key,customer_key order by txn_datetime),0)  as current_balance,
batch_identifier, To_char(CURRENT_TIMESTAMP, 'yyyy-mm-dd hh:mi:ss.ms') ::  timestamp AS insert_datetime
 from (
select c.customer_key, a.account_key, d.date_key, dt.timeofday,txn_datetime,t.opening_balance,t.batch_identifier,
case when credit_debit_ind='C' then txn_amount else -1*txn_amount end txn_amount,
case when t.credit_debit_ind='C' then txn_amount else 0 end as credited_amount,
case when t.credit_debit_ind='D' then txn_amount else 0 end as debited_amount
 from fin_transaction_src t
join dwh_own.dim_account a on t.account_no=a.account_no and a.active_flag='Y'
join dwh_own.dim_customer c on c.customer_id=t.customer_id and c.active_flag='Y'
join dwh_own.dim_calendar_date d on d.date_key=date(t.txn_datetime)
join dwh_own.dim_calendar_time dt on dt.timeofday=to_char(t.txn_datetime,'HH24:MI') 
)txn;

END;