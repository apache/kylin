select CAL_DT,SELLER_ID,lstg_format_name,sum(c1) c1,sum(c2) c2
from
(select CAL_DT,SELLER_ID,lstg_format_name,sum(price) c1,0 c2
from TEST_KYLIN_FACT
where CAL_DT='2012-01-01'
group by CAL_DT,SELLER_ID,lstg_format_name
union all
select CAL_DT,SELLER_ID,lstg_format_name,0 c1,sum(price) c2
from TEST_KYLIN_FACT
where CAL_DT='2012-01-01'
group by CAL_DT,SELLER_ID,lstg_format_name)
group by CAL_DT,SELLER_ID,lstg_format_name