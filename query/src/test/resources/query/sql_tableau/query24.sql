select test_kylin_fact.cal_dt, count(1) as cnt_1
from test_kylin_fact 
left join test_cal_dt on test_kylin_fact.cal_dt=test_cal_dt.cal_dt 
group by test_kylin_fact.cal_dt 
order by 2 desc 
limit 3