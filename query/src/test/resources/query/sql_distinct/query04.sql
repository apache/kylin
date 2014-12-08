select test_cal_dt.week_beg_dt,sum(test_kylin_fact.price) as GMV 
 , count(1) as TRANS_CNT, count(distinct seller_id) as DIST_SELLER 
 from test_kylin_fact 
 inner JOIN edw.test_cal_dt as test_cal_dt  
 ON test_kylin_fact.cal_dt = test_cal_dt.cal_dt 
 where test_kylin_fact.lstg_format_name='FP-GTC' 
 and test_cal_dt.week_beg_dt between DATE '2013-05-01' and DATE '2013-08-01' 
 group by test_cal_dt.week_beg_dt 
 having count(distinct seller_id) > 2 
