select cal_dt, lstg_format_name, sum(price) as GMV 
 from test_kylin_fact 
 where cal_dt=date '2013-05-06' 
 group by cal_dt, lstg_format_name 
