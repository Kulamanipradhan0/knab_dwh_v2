
-- DNB Report Query from Account Balance 

 SELECT Extract(year FROM as_of_date)                AS "Year",
       c.bsn_number                                 AS "Social Security Number",
       c.full_name                                  AS "Customer name",
       Sum(a.closing_balance * ac.weighting_factor) AS "Closing Balance",
       CASE
         WHEN ( Sum(a.closing_balance * ac.weighting_factor) ) >= 100000 THEN
         100000
         ELSE Sum(a.closing_balance * ac.weighting_factor)
       END                                          "Pay Out Amount",
       'EUR'                                        AS "Currency"
FROM   dwh_own.fac_acc_balance a
       LEFT JOIN dwh_own.xref_account_to_customer ac
              ON a.account_key = ac.account_key
       LEFT JOIN dwh_own.dim_customer c
              ON c.customer_key = ac.customer_key
WHERE  as_of_date = (SELECT Max(date_key)
                     FROM   dwh_own.dim_calendar_date
                     WHERE  year = (SELECT Extract(year FROM Now()) - 3)
                            AND week_end = 'Weekday'
                            AND holiday = 'No holiday')
GROUP  BY Extract(year FROM as_of_date),
          c.bsn_number,
          c.full_name
ORDER  BY Extract(year FROM as_of_date),
          c.bsn_number,
          c.full_name;

-- DNB Report Control Query from EOD Position Table using as_of_date=(last day of the year﻿)
-- I have used 2020 as the year, as I had previously created data for 2020
 SELECT Extract(year FROM as_of_date)                AS "Year",
       c.bsn_number                                 AS "Social Security Number",
       c.full_name                                  AS "Customer name",
       Sum(p.closing_balance * ac.weighting_factor) AS "Closing Balance",
       CASE
         WHEN ( Sum(p.closing_balance * ac.weighting_factor) ) >= 100000 THEN
         100000
         ELSE Sum(p.closing_balance * ac.weighting_factor)
       END                                          "Pay Out Amount",
       'EUR'                                        AS "Currency"
FROM   dwh_own.fac_dwh_eod_position p
       LEFT JOIN dwh_own.xref_account_to_customer ac
              ON p.account_key = ac.account_key
       LEFT JOIN dwh_own.dim_customer c
              ON c.customer_key = ac.customer_key
WHERE  as_of_date = (SELECT Max(date_key)
                     FROM   dwh_own.dim_calendar_date
                     WHERE  year = (SELECT Extract(year FROM Now()) - 3)
                            AND week_end = 'Weekday'
                            AND holiday = 'No holiday')
GROUP  BY Extract(year FROM as_of_date),
          c.bsn_number,
          c.full_name
ORDER  BY Extract(year FROM as_of_date),
          c.bsn_number,
          c.full_name;  

--We can create a dashboard to see if both figures match at end of day

  
                
