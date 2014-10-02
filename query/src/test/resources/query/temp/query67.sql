    SELECT SUM(1) AS "COL",   2 AS "COL2" FROM test_kylin_fact
    LEFT JOIN test_cal_dt ON ( test_kylin_fact.cal_dt = test_cal_dt.cal_dt) GROUP BY 2 HAVING COUNT(1)>0