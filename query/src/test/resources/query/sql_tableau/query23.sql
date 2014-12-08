
select fact.cal_dt, sum(fact.price) from test_kylin_fact fact 
left join EDW.TEST_CAL_DT cal on fact.cal_dt=cal.cal_dt
where cal.cal_dt  = date '2012-05-17' or cal.cal_dt  = date '2013-05-17'
group by fact.cal_dt