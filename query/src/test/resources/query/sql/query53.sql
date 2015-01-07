select test_kylin_fact.cal_dt,test_kylin_fact.seller_id, sum(test_kylin_fact.price) as GMV
 , count(*) as TRANS_CNT 
from test_kylin_fact
where DATE '2012-09-01' <= test_kylin_fact.cal_dt   and  test_kylin_fact.seller_id = 10000002
 group by test_kylin_fact.cal_dt,
test_kylin_fact.seller_id