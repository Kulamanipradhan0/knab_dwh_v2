drop table if exists stage_own.fin_txn_a;

create table stage_own.fin_txn_a(
batch_identifier integer not null,
source character varying(10),
date_time timestamp,
account_no character varying(20),
Description character varying(20),
credit_debit_ind char(1),
txn_amount numeric
);
