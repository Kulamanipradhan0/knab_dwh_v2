delete from dwh_own.dim_calendar_date cascade where case when (select count(*) from dwh_own.dim_calendar_date)>0 then 1=2 else 1=1 end ;

--Load all records required for calendar_date table
insert into dwh_own.dim_calendar_date
SELECT 	datum as date_key,
	1 as batch_identifier,
	to_char(datum,'yyyymmdd')::integer  as date,
	extract(year from datum) AS Year,
	extract(month from datum) AS Month,
	-- Localized month name
	to_char(datum, 'TMMonth') AS Month_Name,
	extract(day from datum) AS Day,
	extract(doy from datum) AS Day_Of_Year,
	-- Localized weekday
	to_char(datum, 'TMDay') AS Week_day_Name,
	-- ISO calendar week
	extract(week from datum) AS Calendar_Week,
	'Q' || to_char(datum, 'Q') AS Quartal,
	to_char(datum, 'yyyy/"Q"Q') AS Year_Quartal,
	to_char(datum, 'yyyy/mm') AS Year_Month,
	-- ISO calendar year and week
	to_char(datum, 'iyyy/IW') AS Year_Calendar_Week,
	-- Weekend
	CASE WHEN extract(isodow from datum) in (6, 7) THEN 'Weekend' ELSE 'Weekday' END AS Week_end,
	-- Fixed holidays 
        CASE WHEN to_char(datum, 'MMDD') IN ('0101', '0704', '1225', '1226')
		THEN 'Holiday' ELSE 'No holiday' END
		AS Holiday,
	-- Some periods of the year, adjust for your organisation and country
	CASE WHEN to_char(datum, 'MMDD') BETWEEN '0701' AND '0831' THEN 'Summer break'
	     WHEN to_char(datum, 'MMDD') BETWEEN '1115' AND '1225' THEN 'Christmas season'
	     WHEN to_char(datum, 'MMDD') > '1225' OR to_char(datum, 'MMDD') <= '0106' THEN 'Winter break'
		ELSE 'Normal' END
		AS Period,
	-- ISO start and end of the week of this date
	datum + (1 - extract(isodow from datum))::integer AS cweek_Start,
	datum + (7 - extract(isodow from datum))::integer AS cweek_End,
	-- Start and end of the month of this date
	datum + (1 - extract(day from datum))::integer AS Month_Start,
	(datum + (1 - extract(day from datum))::integer + '1 month'::interval)::date - '1 day'::interval AS Month_End
FROM (
	-- There are 3 leap years in this range, so calculate 365 * 10 + 3 records
	SELECT '1980-01-01'::DATE + sequence.day AS datum
	FROM generate_series(0,500000) AS sequence(day)
	where '1980-01-01'::DATE + sequence.day<='2199-12-31' and  case when (select count(*) from dwh_own.dim_calendar_date)>0 then 1=2 else 1=1 end 
	GROUP BY sequence.day
     ) DQ
order by date_key;

