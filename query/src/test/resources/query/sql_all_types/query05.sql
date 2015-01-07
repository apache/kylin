select ML_double, sum(price) as GMV from test_kylin_fact group by ML_double order by ML_double 
