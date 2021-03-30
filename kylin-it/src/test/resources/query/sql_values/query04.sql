SELECT *
FROM (
	SELECT leaf_categ_id, SUM(price) AS sum_price
	FROM test_kylin_fact
	GROUP BY leaf_categ_id
	UNION ALL
	SELECT leaf_categ_id, SUM(price) AS sum_price
	FROM test_kylin_fact
	GROUP BY leaf_categ_id
)
	CROSS JOIN (
		SELECT MAX(price) AS sum_price_2
		FROM test_kylin_fact
		GROUP BY leaf_categ_id
	)
UNION ALL
SELECT *
FROM (
	SELECT cast(1  as bigint) AS leaf_categ_id, SUM(price) AS sum_price
	FROM test_kylin_fact
	WHERE 1 <> 1
	GROUP BY leaf_categ_id
	UNION ALL
	SELECT cast(2  as bigint) AS leaf_categ_id, SUM(price) AS sum_price
	FROM test_kylin_fact
	WHERE 1 <> 1
	GROUP BY leaf_categ_id
)
	CROSS JOIN (
		SELECT MAX(price) AS sum_price_2
		FROM test_kylin_fact
		GROUP BY leaf_categ_id
	)
UNION ALL
SELECT *
FROM (
	SELECT leaf_categ_id, SUM(price) AS sum_price
	FROM test_kylin_fact
	GROUP BY leaf_categ_id
	UNION ALL
	SELECT leaf_categ_id, SUM(price) AS sum_price
	FROM test_kylin_fact
	GROUP BY leaf_categ_id
)
	CROSS JOIN (
		SELECT MAX(price) AS sum_price_2
		FROM test_kylin_fact
		GROUP BY leaf_categ_id
	)
ORDER BY 1