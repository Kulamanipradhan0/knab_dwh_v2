drop table if exists stage_own.acnt;

create table stage_own.acnt(
batch_identifier integer not null,
source character varying(10),
txn_date date,
account_no character varying(20),
customer_id	character varying(10),
account_type character varying(10),
opening_balance numeric,
closing_balance numeric
);
