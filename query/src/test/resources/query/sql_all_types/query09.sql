select ML_char, sum(price) as GMV from test_kylin_fact group by ML_char order by ML_char 
