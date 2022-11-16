select a.o_orderkey, b.o_orderkey
from TPCH.ORDERS a inner join TPCH.ORDERS b
on a.o_orderkey = b.o_orderkey