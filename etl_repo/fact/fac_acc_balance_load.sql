BEGIN ; 

delete from dwh_own.fac_acc_balance where batch_identifier=
(select distinct batch_identifier from stage_own.acnt );

--Load Fac Account Balance daily Snapshot table

insert into dwh_own.fac_acc_balance
(account_key, as_of_date, opening_balance, closing_balance, batch_identifier, insert_datetime)
(select distinct da.account_key, a.txn_date as_of_date, a.opening_balance, a.closing_balance ,a.batch_identifier,
To_char(CURRENT_TIMESTAMP, 'yyyy-mm-dd hh:mi:ss.ms') ::  timestamp AS ins
from stage_own.acnt a
left join dwh_own.dim_account da on a.account_no=da.account_no and da.active_flag='Y'
left join dwh_own.dim_calendar_date d on d.date_key=a.txn_date
);

END;
