SELECT a.TRANS_CODE AS TRANS_CODE,
       a.TRANS_ID AS ID,
       a.PRICE_A
FROM
  (SELECT TRANS_ID AS TRANS_CODE,
          TRANS_ID,
          round(cast(PRICE/10 AS decimal),2) AS PRICE_A
   FROM TEST_KYLIN_FACT
   WHERE substring(cast(TRANS_ID AS char(6))
                   FROM 1
                   FOR 4) =
       (SELECT max(substring(cast(TRANS_ID AS char(6))
                             FROM 1
                             FOR 4))
        FROM TEST_KYLIN_FACT)) a
LEFT JOIN
  (SELECT TRANS_ID
   FROM TEST_KYLIN_FACT) b ON a.TRANS_ID=b.TRANS_ID
ORDER BY a.TRANS_ID;