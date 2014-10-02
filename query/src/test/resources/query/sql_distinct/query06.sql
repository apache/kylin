select lstg_format_name, 
 sum(price) as GMV, 
 count(1) as TRANS_CNT, 
 count(distinct seller_id) as DIST_SELLER 
 from test_kylin_fact 
 where lstg_format_name='FP-GTC' 
 group by lstg_format_name 
 order by lstg_format_name 
