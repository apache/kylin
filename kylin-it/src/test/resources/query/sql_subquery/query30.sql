
SELECT t1.cal_dt, t1.sum_price,t1.lstg_site_id 
FROM (
  select cal_dt, lstg_site_id, sum(price) as sum_price
  from test_kylin_fact
  group by cal_dt, lstg_site_id
  
) t1

inner JOIN edw.test_cal_dt as test_cal_dt
on t1.cal_dt=test_cal_dt.cal_dt

inner JOIN edw.test_sites as test_sites
on t1.lstg_site_id = test_sites.site_id

