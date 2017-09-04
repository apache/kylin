-- union subquery under join
select count(*) as cnt
FROM TEST_KYLIN_FACT as TEST_A
join (
    select * from TEST_KYLIN_FACT where CAL_DT < DATE '2012-08-01'
    union
    select * from TEST_KYLIN_FACT where CAL_DT > DATE '2013-06-01'
) TEST_B
on TEST_A.ORDER_ID = TEST_B.ORDER_ID
group by TEST_A.SELLER_ID
