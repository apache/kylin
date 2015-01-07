select lstg_format_name, sum(price) as GMV 
 from test_kylin_fact 
 where lstg_format_name='FP-GTC' 
 group by lstg_format_name 
