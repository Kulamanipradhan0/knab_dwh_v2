drop table if exists dwh_own.dim_account CASCADE;

create table dwh_own.dim_account(
account_key	bigserial not null,
batch_identifier integer not null,
effective_start_date date not null ,
effective_end_date date default '9999-12-31'::date not null,
insert_datetime timestamp without time zone  not null,
last_update_datetime timestamp without time zone  not null,
active_flag char(1) default 'Y' not null,
account_no character varying(20) not null,
account_type character varying(20) not null,
CONSTRAINT dim_account_pkey PRIMARY KEY (account_key)
);

create index dim_account_ix_01 on dwh_own.dim_account (active_flag,account_no);

create index dim_account_ix_02 on dwh_own.dim_account (effective_start_date,effective_end_date,account_no);
