/* SCD Type-2 Implementation for dim_customer*/

BEGIN;

drop table IF EXISTS customer;
drop table IF EXISTS active_dim_customer;

--Create Temporary Table for customer

CREATE TEMP TABLE customer AS
(
SELECT customer_id,bsn_number,
                first_name||' '||last_name as full_name,
                address||' ,'||post_code as address,
		coalesce(country,'1') as country,
		coalesce(email,'1') as email,
                batch_identifier
FROM   stage_own.crm_a 
union
SELECT customer_id,bsn_number,
                first_name||' '||last_name as full_name,
                address||' ,'||post_code as address,
		coalesce(country,'1') as country,
		coalesce(email,'1') as email,
                batch_identifier
FROM   stage_own.crm_b
);

--Create Temporary Table for active dim_customer

CREATE TEMP TABLE active_dim_customer AS
(
SELECT customer_id,
       bsn_number,
       full_name, 
       address,
	coalesce(country,'1') as country,
	coalesce(email,'1') as email,
       customer_key 
FROM   dwh_own.dim_customer
WHERE  active_flag = 'Y');

-- New customer Load to Dimension
with 
src as(select * from customer)  ,
tgt as (select * from active_dim_customer)
insert into dwh_own.dim_customer (customer_id,bsn_number,full_name,address,country,email,effective_start_date, insert_datetime, last_update_datetime,batch_identifier)
select 
src.customer_id,
src.bsn_number,
src.full_name,
src.address,
src.country,
src.email,
To_char(CURRENT_TIMESTAMP, 'yyyy-mm-dd') :: date AS start_date,
To_char(CURRENT_TIMESTAMP, 'yyyy-mm-dd hh:mi:ss.ms') ::  timestamp AS insert_datetime,
To_char(CURRENT_TIMESTAMP, 'yyyy-mm-dd hh:mi:ss.ms') ::  timestamp AS last_update_datetime,
batch_identifier
from 
src left join tgt on
src.customer_id=tgt.customer_id
where tgt.customer_id is null --New Customers
or
(tgt.customer_id is not null 
and md5(src.full_name||','||src.address||','||src.country||','||src.email)!=md5(tgt.full_name||','|| tgt.address||','||tgt.country||','||tgt.email)); 
--Existing customers with a SCD change in address & fullname

/*
-- Update changed customer to inactive 
update dwh_own.dim_customer tgt
set last_update_datetime=To_char(CURRENT_TIMESTAMP, 'yyyy-mm-dd hh:mi:ss.ms')::  timestamp, active_flag='N', effective_end_date=To_char(CURRENT_TIMESTAMP, 'yyyy-mm-dd') :: date 
from 
customer src 
where 
src.customer_id=tgt.customer_id 
and (md5(src.full_name||','||src.address||','||src.country||','||src.email)!=md5(tgt.full_name||','|| tgt.address||','||coalesce(tgt.country,'1')||','||coalesce(tgt.email,'1'))) 
and tgt.active_flag='Y';
--Inactive Old records where we have a SCD change
*/

-- 1. Inactive records where there is a SCD change 2. if client has been left the bank i.e. not comign from source
with 
src as(select * from customer)  ,
tgt as (select * from active_dim_customer)
update dwh_own.dim_customer main set last_update_datetime=To_char(CURRENT_TIMESTAMP, 'yyyy-mm-dd hh:mi:ss.ms')::  timestamp, active_flag='N', effective_end_date=To_char(CURRENT_TIMESTAMP, 'yyyy-mm-dd') :: date 
from 
src right join tgt
on src.customer_id=tgt.customer_id  
where (src.customer_id is null 
or (md5(src.full_name||','||src.address||','||src.country||','||src.email)!=md5(tgt.full_name||','|| tgt.address||','||coalesce(tgt.country,'1')||','||coalesce(tgt.email,'1'))))
and main.customer_key=tgt.customer_key;


END;


