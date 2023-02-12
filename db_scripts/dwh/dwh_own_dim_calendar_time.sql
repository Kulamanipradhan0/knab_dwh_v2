DROP TABLE IF EXISTS dwh_own.dim_calendar_time;

CREATE TABLE dwh_own.dim_calendar_time
(
  timeofday character varying(5),
  hour int,
  quarterhour character varying(15),
  minute int,
  daytimename character varying(20),
  daynight character varying(20),
  CONSTRAINT dim_calendar_time_pkey PRIMARY KEY (timeofday)
)
WITH (
  OIDS=FALSE
);