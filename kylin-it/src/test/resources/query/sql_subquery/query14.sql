select
    lstg_format_name,
    sum(price) as gvm
from
    (
        select
            cal_dt,
            lstg_format_name,
            price
        from test_kylin_fact
            inner JOIN test_category_groupings
            ON test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id AND test_kylin_fact.lstg_site_id = test_category_groupings.site_id
            inner JOIN edw.test_sites as test_sites
            ON test_kylin_fact.lstg_site_id = test_sites.site_id
        where
            lstg_site_id = 0
            and cal_dt > '2013-05-13'
    ) f
where
    lstg_format_name ='Auction'
group by
    lstg_format_name