select lstg_format_name, sum(price) as GMV 
 from test_kylin_fact 
 where (NOT ((CASE WHEN (lstg_format_name IS NULL) THEN 1 WHEN NOT (lstg_format_name IS NULL) THEN 0 ELSE NULL END) <> 0))
 group by lstg_format_name 
