SELECT 
 test_category_groupings.meta_categ_name 
 ,sum(test_kylin_fact.price) as GMV 
 ,count(*) as trans_cnt 
 FROM test_kylin_fact 
 inner JOIN test_category_groupings 
 ON test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id 
 AND test_kylin_fact.lstg_site_id = test_category_groupings.site_id 
 group by 
 test_category_groupings.meta_categ_name 
