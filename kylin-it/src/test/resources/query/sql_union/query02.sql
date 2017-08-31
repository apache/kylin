-- union subquery under join
select sum(TEST_A.PRICE) as ITEM_CNT
FROM TEST_KYLIN_FACT as TEST_A
join (
    select * from TEST_KYLIN_FACT where CAL_DT < DATE '2012-06-01'
    union
    select * from TEST_KYLIN_FACT where CAL_DT > DATE '2013-06-01'
) TEST_B
on TEST_A.TRANS_ID = TEST_B.TRANS_ID
group by TEST_A.SELLER_ID
