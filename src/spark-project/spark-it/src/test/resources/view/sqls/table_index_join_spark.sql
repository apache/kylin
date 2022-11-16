--expected_model_hit=orders_join_customer,orders_model;
select
sum(o1.O_TOTALPRICE),
sum(o2.O_TOTALPRICE)
from TPCH.ORDERS o1 inner join tpch.ORDERS o2
on o1.O_ORDERKEY = o2.O_ORDERKEY
where
o1.O_ORDERSTATUS = 'P'
group by o1.O_ORDERDATE