SELECT SUM(PRICE * ITEM_COUNT), TEST_KYLIN_FACT.SELLER_ID
FROM
TEST_KYLIN_FACT INNER JOIN TEST_ACCOUNT ON SELLER_ID = ACCOUNT_ID
INNER JOIN EDW.TEST_CAL_DT ON TEST_KYLIN_FACT.CAL_DT>=TEST_CAL_DT.CAL_DT AND TEST_KYLIN_FACT.CAL_DT IS NOT NULL
GROUP BY TEST_KYLIN_FACT.SELLER_ID


