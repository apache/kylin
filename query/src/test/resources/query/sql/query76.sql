select lstg_format_name, sum(price) as GMV 
 from test_kylin_fact 
 group by lstg_format_name, SLR_SEGMENT_CD
  having SLR_SEGMENT_CD > 0
