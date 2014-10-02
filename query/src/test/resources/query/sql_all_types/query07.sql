select ML_boolean, sum(price) as GMV from test_kylin_fact group by ML_boolean order by ML_boolean 
