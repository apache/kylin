select ML_float, sum(price) as GMV from test_kylin_fact group by ML_float order by ML_float 
