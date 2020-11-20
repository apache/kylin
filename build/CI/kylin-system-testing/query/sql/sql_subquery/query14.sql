select
    lstg_format_name,
    sum(price) as gvm
from
    (
        select
            part_dt,
            lstg_format_name,
            price
        from KYLIN_SALES
            inner JOIN kylin_category_groupings
            ON KYLIN_SALES.leaf_categ_id = kylin_category_groupings.leaf_categ_id AND KYLIN_SALES.lstg_site_id = kylin_category_groupings.site_id

        where
            lstg_site_id = 0
            and part_dt > '2013-05-13'
    ) f
where
    lstg_format_name ='Auction'
group by
    lstg_format_name