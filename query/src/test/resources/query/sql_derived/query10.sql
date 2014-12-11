SELECT 
 test_cal_dt.week_beg_dt 
 ,test_category_groupings.meta_categ_name 
 ,test_category_groupings.upd_user 
 ,test_category_groupings.upd_date 
 ,test_kylin_fact.leaf_categ_id 
 ,test_category_groupings.leaf_categ_id 
 ,test_kylin_fact.lstg_site_id 
 ,test_category_groupings.site_id 
 ,sum(price) as GMV, count(*) as TRANS_CNT 
 FROM test_kylin_fact 
 inner JOIN edw.test_cal_dt as test_cal_dt 
 ON test_kylin_fact.cal_dt = test_cal_dt.cal_dt 
 inner JOIN test_category_groupings 
 ON test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id 
 AND test_kylin_fact.lstg_site_id = test_category_groupings.site_id 
 where 
 test_category_groupings.upd_date='2012-09-11 20:26:04' 
 group by test_cal_dt.week_beg_dt 
 ,test_category_groupings.meta_categ_name 
 ,test_category_groupings.upd_user 
 ,test_category_groupings.upd_date 
 ,test_kylin_fact.leaf_categ_id 
 ,test_category_groupings.leaf_categ_id 
 ,test_kylin_fact.lstg_site_id 
 ,test_category_groupings.site_id 
