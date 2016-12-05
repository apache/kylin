
select meta_categ_name, count(1) as cnt, sum(price) as GMV 

 from test_kylin_fact 
 left JOIN edw.test_cal_dt as test_cal_dt
 ON test_kylin_fact.cal_dt = test_cal_dt.cal_dt
 left JOIN test_category_groupings
 ON test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id AND test_kylin_fact.lstg_site_id = test_category_groupings.site_id
 left JOIN edw.test_sites as test_sites
 ON test_kylin_fact.lstg_site_id = test_sites.site_id

 where not ( meta_categ_name not in ('', 'a','Computers') and meta_categ_name not in ('Crafts','Computers'))
 group by meta_categ_name 