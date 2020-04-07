--ISSUE #6666 support non-equi join router to pushdown
select test_cal_dt.week_beg_dt, sum(test_kylin_fact.price) as GMV
 , count(*) as TRANS_CNT , sum(test_kylin_fact.item_count) as total_items
 from test_kylin_fact
left JOIN edw.test_cal_dt as test_cal_dt
 ON test_kylin_fact.cal_dt = test_cal_dt.cal_dt and test_kylin_fact.lstg_format_name='FP-GTC'
 where
  test_cal_dt.week_beg_dt between '2013-05-01' and DATE '2013-08-01'
 group by test_cal_dt.week_beg_dt