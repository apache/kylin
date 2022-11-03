--expected_model_hit=orders_model_clone;
select sum(O_TOTALPRICE) from TPCH.orders_model_clone where O_ORDERSTATUS = 'P' group by O_ORDERDATE