drop table if exists dwh_own.fac_dwh_eod_position;

create table dwh_own.fac_dwh_eod_position(
account_key bigint NOT NULL REFERENCES dwh_own.dim_account,
as_of_date date NOT NULL REFERENCES dwh_own.dim_calendar_date,
opening_balance numeric,
closing_balance numeric,
batch_identifier integer NOT NULL ,
insert_datetime timestamp without time zone default To_char(CURRENT_TIMESTAMP, 'yyyy-mm-dd hh:mi:ss.ms') ::timestamp not null,
CONSTRAINT fac_dwh_eod_position_pkey PRIMARY KEY (account_key,as_of_date)
);

