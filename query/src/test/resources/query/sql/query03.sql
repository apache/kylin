select test_kylin_fact.lstg_format_name, sum(price) as GMV, count(*) as TRANS_CNT from test_kylin_fact 
 group by test_kylin_fact.lstg_format_name 
