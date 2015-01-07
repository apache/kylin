select cal_dt, lstg_format_name, sum(price) as GMV 
 from test_kylin_fact 
 where cal_dt between date '2013-05-06' and date '2013-07-31' 
 group by cal_dt, lstg_format_name 
