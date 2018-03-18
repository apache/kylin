SELECT LSTG_FORMAT_NAME, sum_price, sum_price2
FROM (
	SELECT LSTG_FORMAT_NAME, SUM(price) AS sum_price
	FROM test_kylin_fact
	GROUP BY LSTG_FORMAT_NAME
	UNION ALL
	SELECT LSTG_FORMAT_NAME, SUM(price) AS sum_price
	FROM test_kylin_fact
	GROUP BY LSTG_FORMAT_NAME
)
	CROSS JOIN (
		SELECT SUM(price) AS sum_price2
		FROM test_kylin_fact
		GROUP BY LSTG_FORMAT_NAME
	)
UNION ALL
SELECT 'name' AS LSTG_FORMAT_NAME, 11.2 AS sum_price, 22.1 AS sum_price2