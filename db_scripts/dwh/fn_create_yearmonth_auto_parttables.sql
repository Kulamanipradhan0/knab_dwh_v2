CREATE OR REPLACE FUNCTION dwh_own.fn_create_yearmonth_auto_parttables(p_schemaname text, p_tablename text, p_date_column text, p_yearmonth text, p_indexspace text)
 RETURNS text
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$ 
DECLARE
  var_return integer;
  var_teller integer;
  var_create varchar(25);
  var_count integer;
  var_owner varchar(32);
  var_schematable text;
  var_pk_colname varchar(300);
  var_pk_name varchar(300);
  var_partname varchar(90);
  var_grantcount RECORD;
  var_indexcount RECORD;
  var_index_colname varchar(300);
  var_index_name varchar(300);
  var_total_duration text;
  var_start_time timestamp without time zone := clock_timestamp();
    
BEGIN
  var_owner := p_schemaname;
  var_partname := p_tablename || '_' || p_yearmonth;
  var_schematable := var_owner||'.'||p_tablename;
	
  ---- Check if the partition already exists ---
  SELECT 1 INTO var_teller
      from information_schema.tables where table_schema = var_owner and table_name = lower(var_partname);
    
  IF var_teller is null
  THEN
     
	-- Find Primary keys of the table   
	SELECT tc.constraint_name, string_agg(kcu.column_name, ', ') INTO var_pk_name,var_pk_colname
	from information_schema.table_constraints tc,
	(	select constraint_schema, table_name, constraint_name, column_name, 
			ordinal_position, position_in_unique_constraint, table_schema 
		from information_schema.key_column_usage 
		where table_schema = p_schemaname 
		and table_name = p_tablename
		order by ordinal_position desc, position_in_unique_constraint 
	) kcu
	where tc.constraint_name = kcu.constraint_name
	and tc.table_schema = kcu.table_schema
	and tc.table_name = p_tablename 
	and tc.constraint_type = 'PRIMARY KEY'
	group by tc.constraint_name;

        
	--Creating Partitions
        EXECUTE format('CREATE TABLE %I.%I ( CHECK (%s >= %s01::int AND %s <= %s31::int) ) INHERITS ( %s );',
				var_owner, var_partname, p_date_column, p_yearmonth, p_date_column, p_yearmonth, var_schematable);

        -- checking primary key is present or not
	IF var_pk_colname = '' OR var_pk_colname IS NULL
	THEN 
		RAISE NOTICE 'No Primary key found!';
	ELSE
		EXECUTE format('ALTER TABLE %I.%I ADD CONSTRAINT %s_%s PRIMARY KEY (%s);', 
					var_owner, var_partname, var_pk_name, p_yearmonth, var_pk_colname);
	END IF;

	-- Creating Grant for schemas
	FOR var_grantcount IN
		SELECT grantee, string_agg(privilege_type, ', ') as privilage FROM information_schema.role_table_grants 
		WHERE table_name=p_tablename  and table_schema=var_owner GROUP BY grantee 
	LOOP
		EXECUTE format('GRANT %s ON TABLE %I.%I TO %s;', var_grantcount.privilage, var_owner, var_partname, var_grantcount.grantee);
	END LOOP;
	

	-- Creating Indexes for the Partitioned tables

	FOR var_indexcount IN 
		SELECT i.relname as indname,
		       replace(replace(ARRAY(
		       SELECT pg_get_indexdef(idx.indexrelid, k + 1, true)
		       FROM generate_subscripts(idx.indkey, 1) as k
		       ORDER BY k
		       )::text,'{',''),'}','') as indkey_names, idx.indisunique 
		FROM   pg_index as idx
		JOIN   pg_class as i
		ON     i.oid = idx.indexrelid
		JOIN   pg_am as am
		ON     i.relam = am.oid
		WHERE idx.indrelid::regclass=lower(var_schematable)::regclass
		AND    not idx.indisprimary
	LOOP
		IF var_indexcount.indisunique THEN
			EXECUTE format('CREATE UNIQUE INDEX %s_%s ON %I.%I USING btree (%s) TABLESPACE %s;', 
						var_indexcount.indname, p_yearmonth, var_owner, var_partname, var_indexcount.indkey_names, p_indexspace);
		ELSE
			EXECUTE format('CREATE INDEX %s_%s ON %I.%I USING btree (%s) TABLESPACE %s;', 
						var_indexcount.indname, p_yearmonth, var_owner, var_partname, var_indexcount.indkey_names, p_indexspace);
		END IF;
	END LOOP;
	

        --Insert into partition log
	var_total_duration := clock_timestamp() - var_start_time;

	RAISE NOTICE '% Partition created', var_partname;
	
	END IF;
  
  RETURN var_return;
END;
$function$
;

