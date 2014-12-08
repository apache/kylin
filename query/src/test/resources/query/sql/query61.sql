select count(1) as cnt_1
from test_kylin_fact 
left JOIN edw.test_cal_dt as test_cal_dt on test_kylin_fact.cal_dt=test_cal_dt.cal_dt 
group by test_kylin_fact.cal_dt 
order by 1 desc 
limit 4