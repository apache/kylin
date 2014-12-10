SELECT SUM(1) AS "COL", 
 2 AS "COL2" 
 FROM ( 
 select test_kylin_fact.lstg_format_name, test_cal_dt.week_beg_dt,sum(test_kylin_fact.price) as GMV 
 , count(*) as TRANS_CNT 
 from test_kylin_fact 
 inner JOIN edw.test_cal_dt as test_cal_dt
 ON test_kylin_fact.cal_dt = test_cal_dt.cal_dt 
 where test_cal_dt.week_beg_dt between DATE '2013-05-01' and DATE '2013-08-01' 
 group by test_kylin_fact.lstg_format_name, test_cal_dt.week_beg_dt 
 having sum(price)>500 
 ) "TableauSQL" 
 GROUP BY 2 
 HAVING COUNT(1)>0 
 
