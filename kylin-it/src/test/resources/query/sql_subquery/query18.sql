

select test_kylin_fact.lstg_format_name, sum(test_kylin_fact.price) as GMV 
 , count(*) as TRANS_CNT
 from  

 test_kylin_fact

-- inner JOIN test_category_groupings
-- ON test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id AND test_kylin_fact.lstg_site_id = test_category_groupings.site_id 

 inner JOIN (select leaf_categ_id, site_id,meta_categ_name from test_category_groupings ) yyy
 ON test_kylin_fact.leaf_categ_id = yyy.leaf_categ_id AND test_kylin_fact.lstg_site_id = yyy.site_id 


 inner JOIN (select cal_dt,week_beg_dt from edw.test_cal_dt  where week_beg_dt >= DATE '2010-02-10'  ) xxx
 ON test_kylin_fact.cal_dt = xxx.cal_dt 
 
 
 where yyy.meta_categ_name > ''
 group by test_kylin_fact.lstg_format_name


