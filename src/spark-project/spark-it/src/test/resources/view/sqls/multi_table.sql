--expected_model_hit=orders_join_customer;
select
sum(O_TOTALPRICE+1), -- CC
sum(CC1), -- CC
SUM(C_ACCTBAL_CUSTOMER_1)
from TPCH.orders_join_customer
where
C_ACCTBAL_CUSTOMER_1 > 5000 -- derived dim
group by O_ORDERDATE