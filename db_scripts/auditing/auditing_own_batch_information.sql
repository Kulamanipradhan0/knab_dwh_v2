DROP TABLE IF EXISTS auditing_own.process_information CASCADE;
DROP TABLE IF EXISTS auditing_own.error_log CASCADE;
DROP TABLE IF EXISTS auditing_own.batch_information;

CREATE TABLE auditing_own.batch_information (
	batch_identifier serial NOT NULL,
	batch_name text NULL,
	environment text NULL,
	business_date integer NULL,
	batch_version smallint not null,
	batch_success_flag char(1) NULL,
	batch_start_time timestamp NULL,
	batch_end_time timestamp NULL,
	batch_status text NULL,
	monthly_batch_flag char(1) NULL,
	CONSTRAINT batch_information_pkey PRIMARY KEY (batch_identifier)
);
