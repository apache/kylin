-- unionall
select max(ORDER_ID) as ORDER_ID_MAX
from TEST_KYLIN_FACT as TEST_A
where ORDER_ID <> 1
union all
select max(ORDER_ID) as ORDER_ID_MAX
from TEST_KYLIN_FACT as TEST_B