-- union subquery under join
select  count(*) as cnt,TEST_A.SELLER_ID
FROM TEST_KYLIN_FACT as TEST_A
inner join (

    (
    select sum(PRICE) as x, seller_id,count(*) as y
    from 
        TEST_KYLIN_FACT where CAL_DT < DATE '2012-08-01'
    group by seller_id
    )
   
       union
       
       (
    select sum(PRICE) as x, seller_id,count(*) as y
        from 
            TEST_KYLIN_FACT where CAL_DT > DATE '2012-12-01'
        group by seller_id
       )
        
) TEST_B
on TEST_A.seller_id = TEST_B.seller_id
group by TEST_A.seller_id
