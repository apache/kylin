SELECT C_REGION, DATES.D_DATEKEY, SUM(LINEORDER.LO_EXTENDEDPRICE * LINEORDER.LO_DISCOUNT)
FROM SSB.P_LINEORDER AS LINEORDER
         INNER JOIN SSB.DATES ON LINEORDER.LO_ORDERDATE = DATES.D_DATEKEY
         INNER JOIN SSB.CUSTOMER ON LINEORDER.LO_CUSTKEY = CUSTOMER.C_CUSTKEY
GROUP BY C_REGION, DATES.D_DATEKEY
ORDER BY C_REGION, DATES.D_DATEKEY