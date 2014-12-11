select test_cal_dt.QTR_BEG_DT,sum(test_kylin_fact.price) as gmv 
 , count(*) as trans_cnt 
 from test_kylin_fact 
 inner JOIN edw.test_cal_dt as test_cal_dt 
 ON test_kylin_fact.cal_dt = test_cal_dt.cal_dt 
 where test_kylin_fact.lstg_format_name='FP-GTC' 
 and test_cal_dt.week_beg_dt between '2013-05-01' and '2013-08-01' 
 group by test_cal_dt.QTR_BEG_DT 
