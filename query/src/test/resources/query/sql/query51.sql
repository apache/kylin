select test_kylin_fact.lstg_format_name, test_cal_dt.week_beg_dt,sum(test_kylin_fact.price) as GMV 
 , count(*) as TRANS_CNT 
 from test_kylin_fact 
 inner JOIN edw.test_cal_dt as test_cal_dt 
 ON test_kylin_fact.cal_dt = test_cal_dt.cal_dt 
 where  DATE '2013-03-24'  <= test_cal_dt.week_beg_dt
 group by test_kylin_fact.lstg_format_name, test_cal_dt.week_beg_dt 
