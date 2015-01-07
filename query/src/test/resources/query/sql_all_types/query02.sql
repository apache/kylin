select ML_int, sum(price) as GMV from test_kylin_fact group by ML_int order by ML_int 
