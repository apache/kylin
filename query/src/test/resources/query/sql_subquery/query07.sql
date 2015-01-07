select cal_dt, sum(price) as sum_price
from test_kylin_fact fact
inner join (
select count(1) as cnt, min(cal_dt) as "mmm",  cal_dt as dt from test_kylin_fact group by cal_dt order by 2 desc limit 10
) t0 on (fact.cal_dt = t0.dt) 
group by cal_dt