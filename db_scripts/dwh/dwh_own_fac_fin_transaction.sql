
create table dwh_own.fac_fin_transaction(
customer_key bigint NOT NULL REFERENCES dwh_own.dim_customer,
account_key bigint NOT NULL REFERENCES dwh_own.dim_account,
txn_date date NOT NULL REFERENCES dwh_own.dim_calendar_date,
txn_time_hhmm character varying(5) NOT NULL REFERENCES dwh_own.dim_calendar_time,
txn_datetime_full text NOT NULL ,
credited_amount numeric,
debited_amount numeric,
current_balance numeric, 
batch_identifier integer NOT NULL ,
insert_datetime timestamp without time zone default To_char(CURRENT_TIMESTAMP, 'yyyy-mm-dd hh:mi:ss.ms') ::timestamp not null,
CONSTRAINT fac_fin_transaction_pkey PRIMARY KEY (customer_key,account_key,txn_datetime_full)
);

create index fac_fin_transaction_ix_01 on dwh_own.fac_fin_transaction (batch_identifier);
