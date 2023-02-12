drop table if exists stage_own.crm_b;

create table stage_own.crm_b(
batch_identifier integer not null,
source character varying(10),
customer_id	character varying(10),
bsn_number numeric,
first_name character varying(256),
last_name character varying(256),
address character varying(256),
country character varying(50),
post_code character varying(30),
email character varying(50)
);
