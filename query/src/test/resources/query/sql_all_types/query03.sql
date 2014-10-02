select ML_bigint, sum(price) as GMV from test_kylin_fact group by ML_bigint order by ML_bigint 
