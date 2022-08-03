select '>' + space(LSTG_SITE_ID)+ '<' from (
select LSTG_SITE_ID from TEST_KYLIN_FACT where LSTG_SITE_ID=15 limit 1)