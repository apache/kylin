select ML_tinyint, sum(price) as GMV from test_kylin_fact group by ML_tinyint order by ML_tinyint 
