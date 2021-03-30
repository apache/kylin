select leaf_categ_id, sum(price) as sum_price from test_kylin_fact  group by leaf_categ_id
union all
select cast(1  as bigint) as leaf_categ_id, 22.5 as sum_price