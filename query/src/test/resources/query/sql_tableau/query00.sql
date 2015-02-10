select test_cal_dt.week_beg_dt, sum(test_kylin_fact.price) 
 from test_kylin_fact 
 inner join edw.test_cal_dt as test_cal_dt ON test_kylin_fact.cal_dt = test_cal_dt.cal_dt 
 group by test_cal_dt.week_beg_dt 

