select ML_smallint, sum(price) as GMV from test_kylin_fact group by ML_smallint order by ML_smallint 
