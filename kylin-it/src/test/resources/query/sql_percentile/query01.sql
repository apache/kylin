select seller_id, percentile(price, 0.5) from test_kylin_fact
group by seller_id