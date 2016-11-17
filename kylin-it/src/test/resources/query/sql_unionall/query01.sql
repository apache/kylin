-- unionall
select count(ORDER_ID) as ORDER_ID
from TEST_KYLIN_FACT as TEST_A
where ORDER_ID <> 1
union all
select count(ORDER_ID) as ORDER_ID
from TEST_KYLIN_FACT as TEST_B
