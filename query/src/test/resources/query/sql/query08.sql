select test_kylin_fact.lstg_format_name,sum(test_kylin_fact.price) as GMV 
 , count(*) as TRANS_CNT from test_kylin_fact 
 where test_kylin_fact.lstg_format_name='FP-GTC' 
 group by test_kylin_fact.lstg_format_name 
