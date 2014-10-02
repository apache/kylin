select count(*) from ( select test_kylin_fact.lstg_format_name from test_kylin_fact 
 where test_kylin_fact.lstg_format_name='FP-GTC' 
 group by test_kylin_fact.lstg_format_name ) t 
