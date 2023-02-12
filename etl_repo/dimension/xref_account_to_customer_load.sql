/* Implementation for dim_account */
/* Expected source to deliver all active accounts with opening & closing balance everyday */
/* If any account is missing, we will mark it as inactive in our system */
/* Ideally account would be a SCD Type-2 load. As I have only one attribute i.e. account_type hence not implemented */

BEGIN;

drop table if exists joint_account;
drop table if exists account_to_customer_temp;

--Used for rerunning this ETL if needed
delete from dwh_own.xref_account_to_customer
where batch_identifier=(
select distinct batch_identifier from stage_own.acnt
);

create temp table account_to_customer_temp as
select distinct a.batch_identifier,dc.customer_key,dc.customer_id,a.account_no,da.account_key from stage_own.acnt a
left join dwh_own.dim_customer dc on dc.customer_id=a.customer_id and dc.active_flag='Y'
left join dwh_own.dim_account da on da.account_no=a.account_no and da.active_flag='Y';

create temp table joint_account as
select account_no,count(*) no_of_customers from stage_own.acnt a
group by account_no
having count(*)>1;

insert into dwh_own.xref_account_to_customer
select ct.batch_identifier,ct.account_key,ct.customer_key,
(1.0/coalesce(no_of_customers,1))::numeric(5,4) as weighting_factor 
from account_to_customer_temp ct
left join joint_account j on j.account_no=ct.account_no;


END;
