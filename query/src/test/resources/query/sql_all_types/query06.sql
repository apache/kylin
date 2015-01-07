select ML_decimal, sum(price) as GMV from test_kylin_fact group by ML_decimal order by ML_decimal 
