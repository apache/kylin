select lstg_format_name, cal_dt,
 sum(price) as GMV,
 count(1) as TRANS_CNT
 from test_kylin_fact
 group by lstg_format_name, cal_dt