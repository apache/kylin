SELECT COUNT(DISTINCT "TableauSQL"."TRANS_CNT") AS "ctd_TRANS_CNT_qk", "TableauSQL"."LSTG_FORMAT_NAME" AS "none_LSTG_FORMAT_NAME_nk" 
 FROM ( select test_kylin_fact.lstg_format_name, sum(price) as GMV, count(seller_id) as TRANS_CNT 
 from test_kylin_fact 
 group by test_kylin_fact.lstg_format_name 
 ) "TableauSQL" 
 GROUP BY "TableauSQL"."LSTG_FORMAT_NAME" 
