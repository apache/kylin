-- KE-13678
select count(*)
from TEST_KYLIN_FACT
where ((case when item_count=1 then 3 else 1 end) not in (4))

