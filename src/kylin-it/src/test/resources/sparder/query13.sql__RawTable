SELECT
    t1.leaf_categ_id, COUNT(*) AS nums
FROM
    (SELECT
        f.leaf_categ_id
    FROM
        test_kylin_fact f inner join TEST_CATEGORY_GROUPINGS o on f.leaf_categ_id = o.leaf_categ_id and f.LSTG_SITE_ID = o.site_id
    WHERE
        f.lstg_format_name = 'ABIN') t1
    INNER JOIN
    (SELECT
        leaf_categ_id
    FROM
        test_kylin_fact f
    INNER JOIN test_order o ON f.order_id = o.order_id
    WHERE
        buyer_id > 100) t2 ON t1.leaf_categ_id = t2.leaf_categ_id
GROUP BY t1.leaf_categ_id
ORDER BY nums, leaf_categ_id
limit 100