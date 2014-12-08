select test_cal_dt.week_beg_dt, sum(test_kylin_fact.price) as GMV 
 , count(*) as TRANS_CNT 
 from test_kylin_fact 
 inner JOIN edw.test_cal_dt as test_cal_dt 
 ON test_kylin_fact.cal_dt = test_cal_dt.cal_dt 
 where 
 (test_kylin_fact.lstg_format_name > '') 
 and ( 
 (test_kylin_fact.lstg_format_name='FP-GTC') 
 OR 
 (test_cal_dt.week_beg_dt between DATE '2013-05-20' and DATE '2013-05-21') 
 ) 
 and ( 
 (test_kylin_fact.lstg_format_name='ABIN') 
 OR 
 (test_cal_dt.week_beg_dt between DATE '2013-05-20' and DATE '2013-05-21') 
 ) 
 group by test_cal_dt.week_beg_dt 
