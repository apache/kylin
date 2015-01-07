select cal_dt, sum(price)as GMV, count(1) as trans_cnt from test_kylin_fact 
 group by cal_dt 
