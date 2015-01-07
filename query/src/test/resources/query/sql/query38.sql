select lstg_format_name, sum(price) as GMV 
 from test_kylin_fact 
 where lstg_format_name not in ('FP-GTC', 'ABIN') 
 group by lstg_format_name 
