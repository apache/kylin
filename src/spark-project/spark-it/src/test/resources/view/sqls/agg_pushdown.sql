--expected_model_hit=orders_join_customer,orders_model;
select
sum(o1.O_TOTALPRICE+1), -- CC
SUM(o1.C_ACCTBAL_CUSTOMER_1)
from TPCH.orders_join_customer o1 inner join tpch.orders_model o2
on o1.O_ORDERKEY = o2.O_ORDERKEY
where
o1.C_ACCTBAL_CUSTOMER_1 > 5000 -- derived dim
group by o1.O_ORDERDATE