select fact.cal_dt, sum(fact.price) as sum_price, count(1) as cnt_1
from test_kylin_fact fact 
left join edw.test_cal_dt cal on fact.cal_dt=cal.cal_dt
inner join
(
	select test_kylin_fact.cal_dt, sum(test_kylin_fact.price) from test_kylin_fact left join edw.test_cal_dt test_cal_dt
	on test_kylin_fact.cal_dt=test_cal_dt.cal_dt group by test_kylin_fact.cal_dt order by 2 desc limit 7
) cal_2 on fact.cal_dt = cal_2.cal_dt 
group by fact.cal_dt