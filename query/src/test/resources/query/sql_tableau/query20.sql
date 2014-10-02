
 
 
 SELECT "TableauSQL"."LSTG_FORMAT_NAME" AS "none_LSTG_FORMAT_NAME_nk", SUM("TableauSQL"."TRANS_CNT") AS "sum_TRANS_CNT_qk" 
 FROM ( select test_kylin_fact.lstg_format_name, sum(price) as GMV, count(seller_id) as TRANS_CNT 
 from test_kylin_fact where test_kylin_fact.lstg_format_name > 'ab' 
 group by test_kylin_fact.lstg_format_name having count(seller_id) > 2 ) "TableauSQL" 
 GROUP BY "TableauSQL"."LSTG_FORMAT_NAME" 
