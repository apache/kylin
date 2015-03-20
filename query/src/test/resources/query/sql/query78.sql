select sum(PRICE) as GMV, LSTG_FORMAT_NAME as FORMAT_NAME
from test_kylin_fact
where (LSTG_FORMAT_NAME in ('ABIN')) or  (LSTG_FORMAT_NAME>='FP-GTC' and LSTG_FORMAT_NAME<='Others')
group by LSTG_FORMAT_NAME
