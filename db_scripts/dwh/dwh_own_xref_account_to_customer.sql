drop table if exists dwh_own.xref_account_to_customer CASCADE;

create table dwh_own.xref_account_to_customer(
batch_identifier integer not null,
account_key	bigint not null,
customer_key bigint not null,
weighting_factor numeric(5,4) not null,
CONSTRAINT xref_account_to_customer_pkey PRIMARY KEY (batch_identifier,account_key,customer_key),
CONSTRAINT fk_dim_customer      FOREIGN KEY(customer_key) 	  REFERENCES dwh_own.dim_customer(customer_key),
CONSTRAINT fk_dim_account       FOREIGN KEY(account_key) 	  REFERENCES dwh_own.dim_account(account_key)
);