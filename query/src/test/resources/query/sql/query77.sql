select test_kylin_fact.lstg_format_name,sum(test_kylin_fact.price) as GMV
 , count(*) as TRANS_CNT from test_kylin_fact
 group by test_kylin_fact.lstg_format_name having sum(price)>5000 and count(*)>72