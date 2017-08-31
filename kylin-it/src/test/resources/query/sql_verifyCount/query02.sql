-- simple union
select * from TEST_KYLIN_FACT where CAL_DT < DATE '2012-06-01'
union
select * from TEST_KYLIN_FACT where CAL_DT > DATE '2013-07-01'