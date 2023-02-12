
CREATE TABLE auditing_own.error_log (
	batch_identifier int8 not NULL ,
	process_identifier int8 not null,
	process_start_time timestamp not NULL,
	source_system_name text NULL,
	source_file_name varchar(100) NULL,
	primary_key_columns text NULL,
	primary_key_columns_value text NULL,
	error_column text NULL,
	error_column_value text NULL,
	error_description text NULL,
	flag text NULL,
	error_identifier int4 NULL
);
