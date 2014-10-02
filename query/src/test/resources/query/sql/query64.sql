 select test_kylin_fact.lstg_format_name,sum(test_kylin_fact.price) as GMV , min(cal_dt) as min_cal_dt
 , count(*) as TRANS_CNT from test_kylin_fact
 group by test_kylin_fact.lstg_format_name