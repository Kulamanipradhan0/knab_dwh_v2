CREATE OR REPLACE FUNCTION dwh_own.fn_create_yearmonth_auto_partition(p_schemaname text, p_tablename text, p_date_column text, p_indexspace text)
 RETURNS text
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$ 
DECLARE
var_subtrigger_sqlstatement text;
var_grantcount text;
var_return text;

BEGIN

-- Creating the sub function to make partition automatically
var_subtrigger_sqlstatement := 'CREATE OR REPLACE FUNCTION '||p_schemaname||'.part_functrg_for_'||p_tablename||'() 
RETURNS trigger AS '||'$'||'BODY'||'$'||' 
DECLARE 
li_month integer;
partition_tablename text;
var_fun_output text;
BEGIN
li_month := (NEW.'||p_date_column||' / 100)::int;
	BEGIN
		partition_tablename := '||''''||p_tablename||'_'||''''||'||li_month::text;
		EXECUTE FORMAT(''INSERT INTO %I.%I VALUES($1.*)'',TG_TABLE_SCHEMA,partition_tablename) USING NEW; 
	EXCEPTION 
		WHEN SQLSTATE ''42P01'' THEN

		IF li_month IS NOT null THEN
			RAISE NOTICE ''Creating new partition'';
			var_fun_output := '||p_schemaname||'.fn_create_yearmonth_auto_parttables( '||''''||p_schemaname||''''||'::text'||','||''''||p_tablename||''''||'::text'||','||''''||p_date_column
			||''''||'::text'||','||'li_month::text'||','||''''||p_indexspace||''''||'::text'||' );
			EXECUTE FORMAT(''INSERT INTO %I.%I VALUES($1.*)'',TG_TABLE_SCHEMA,partition_tablename) USING NEW;
		ELSE      
			 RAISE exception SQLSTATE ''14400'' using message = ''Invalid data or null value for partition key. Inserted partition key does not map to any partition'';
		END IF;
		 
		RETURN NULL;
	END;
RETURN NULL;

END;'
||' $'||'BODY'||'$ '||'
  LANGUAGE plpgsql ;';
-- ALTER FUNCTION '||p_schemaname||'.part_functrg_for_'||p_tablename||'()
--  OWNER TO '||p_schemaname||';';

EXECUTE var_subtrigger_sqlstatement;
RAISE NOTICE 'I am here';

-- Provide EXECUTE priviledges to the function
FOR var_grantcount IN
	SELECT distinct grantee FROM information_schema.role_table_grants 
	WHERE table_name=p_tablename  and table_schema=p_schemaname 
LOOP
	EXECUTE format('GRANT EXECUTE ON FUNCTION %s.part_functrg_for_%s() TO %s;', p_schemaname, p_tablename, var_grantcount);
END LOOP;


-- Creating the Main trigger function to call the sub trigger function
EXECUTE format('CREATE TRIGGER trg_%s
 BEFORE INSERT '||'
 ON %I.%I
 FOR EACH ROW
  EXECUTE PROCEDURE %I.part_functrg_for_%s();
  ', p_tablename, p_schemaname, p_tablename,p_schemaname, p_tablename);


var_return :='Triggers Successfully created for table '||p_tablename;

  RETURN var_return;
END;
$function$
;

