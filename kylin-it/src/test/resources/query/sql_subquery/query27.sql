

select test_kylin_fact.lstg_format_name, xxx.week_beg_dt , sum(test_kylin_fact.price) as GMV 
 , count(*) as TRANS_CNT 
 from  

 test_kylin_fact

 inner JOIN test_category_groupings
 ON test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id AND test_kylin_fact.lstg_site_id = test_category_groupings.site_id 


 inner JOIN (select cal_dt,week_beg_dt from edw.test_cal_dt  where week_beg_dt >= DATE '2010-02-10'  ) xxx
 ON test_kylin_fact.cal_dt = xxx.cal_dt 


 where test_category_groupings.meta_categ_name  <> 'Baby'
 group by test_kylin_fact.lstg_format_name, xxx.week_beg_dt 


