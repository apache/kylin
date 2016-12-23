select
    f.cal_dt
from test_kylin_fact f
where
    f.cal_dt not in (
        select cal_dt from EDW.TEST_CAL_DT where week_beg_dt = date'2012-01-01'
    )
