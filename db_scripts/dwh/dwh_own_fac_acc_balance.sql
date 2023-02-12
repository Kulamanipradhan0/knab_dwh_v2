
create table dwh_own.fac_acc_balance(
account_key bigint NOT NULL REFERENCES dwh_own.dim_account,
as_of_date date NOT NULL REFERENCES dwh_own.dim_calendar_date,
opening_balance numeric,
closing_balance numeric,
batch_identifier integer NOT NULL ,
insert_datetime timestamp without time zone default To_char(CURRENT_TIMESTAMP, 'yyyy-mm-dd hh:mi:ss.ms') ::timestamp not null,
CONSTRAINT fac_acc_balance_pkey PRIMARY KEY (account_key,as_of_date)
);

create index fac_acc_balance_ix_01 on dwh_own.fac_acc_balance (batch_identifier);