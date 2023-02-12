
CREATE TABLE auditing_own.process_information (
	process_identifier int8 NOT NULL,
	batch_identifier int8 NOT NULL references auditing_own.batch_information,
	process_name varchar(50) NULL,
	process_start_time timestamp NOT NULL,
	process_end_time timestamp NULL,
	process_status text NULL,
	process_status_description text NULL,
	source_system_name text NULL,
	source_file_name varchar(50) NULL,
	number_of_source_data_records numeric(23) NULL,
	number_of_fatal_error_records numeric(23) NULL,
	number_of_warning_records numeric(23) NULL,
	target_table_name text NULL,
	CONSTRAINT process_information_pkey PRIMARY KEY (process_identifier, batch_identifier, process_start_time)
);

