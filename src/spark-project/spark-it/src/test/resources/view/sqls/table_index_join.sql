--expected_model_hit=orders_model,orders_model;
select
sum(o1.O_TOTALPRICE),
sum(o2.O_TOTALPRICE)
from TPCH.orders_model o1 inner join tpch.orders_model o2
on o1.O_ORDERKEY = o2.O_ORDERKEY
where
o1.O_ORDERSTATUS = 'P'
group by o1.O_ORDERDATE