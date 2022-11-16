--expected_model_hit=orders_model,orders_model_clone;
select a.o_orderkey, b.o_orderkey
from TPCH.orders_model a inner join TPCH.orders_model_clone b
on a.o_orderkey = b.o_orderkey
