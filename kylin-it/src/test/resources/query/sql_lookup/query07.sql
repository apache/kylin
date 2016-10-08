select site_id,count(*) as y,count(DISTINCT site_name) as x from edw.test_sites group by site_id
