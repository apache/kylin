SELECT test_sites.site_name, test_kylin_fact.lstg_format_name, sum(test_kylin_fact.price) as GMV, count(*) as TRANS_CNT 
 FROM test_kylin_fact 
 inner JOIN edw.test_sites as test_sites ON test_kylin_fact.lstg_site_id = test_sites.site_id 
 GROUP BY 
 test_sites.site_name, test_kylin_fact.lstg_format_name 
