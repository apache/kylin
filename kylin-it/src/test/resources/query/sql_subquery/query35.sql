select c.SITE_NAME, c.SITE_ID, b.SELLER_ID, b.LSTG_FORMAT_NAME from EDW.TEST_SITES c join (
 select * from TEST_KYLIN_FACT f join EDW.TEST_SITES s on f.LSTG_SITE_ID= s.SITE_ID where f.ORDER_ID <= 3
 ) b on b.LSTG_SITE_ID= c.SITE_ID