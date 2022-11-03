--expected_model_hit=orders_model;
select sum(O_TOTALPRICE) from TPCH.orders_model where O_ORDERSTATUS = 'P' group by O_ORDERDATE