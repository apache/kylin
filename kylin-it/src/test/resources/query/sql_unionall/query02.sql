-- unionall
select count(*) as count_a
from TEST_KYLIN_FACT as TEST_A
where lstg_format_name='FP-GTC'
union all
select count(*) as count_a
from TEST_KYLIN_FACT as TEST_A
where lstg_format_name='FP-GTC'