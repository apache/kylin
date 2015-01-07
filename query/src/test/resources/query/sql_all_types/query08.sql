select ML_varchar, sum(price) as GMV from test_kylin_fact group by ML_varchar order by ML_varchar 
