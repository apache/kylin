select test_kylin_fact.lstg_format_name, test_kylin_fact.cal_dt,sum(test_kylin_fact.price) as GMV
 , count(*) as TRANS_CNT
 from test_kylin_fact
inner JOIN edw.test_cal_dt as test_cal_dt
 ON test_kylin_fact.cal_dt = test_cal_dt.cal_dt
 inner JOIN test_category_groupings
 ON test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id AND test_kylin_fact.lstg_site_id = test_category_groupings.site_id
 inner JOIN edw.test_sites as test_sites
 ON test_kylin_fact.lstg_site_id = test_sites.site_id
 where test_kylin_fact.cal_dt < DATE '2013-10-10' AND test_kylin_fact.cal_dt > DATE '2013-09-10'
 group by test_kylin_fact.lstg_format_name, test_kylin_fact.cal_dt
