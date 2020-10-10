select site_id,count(*) as y,count(DISTINCT site_name) as x from edw.test_sites group by site_id
;{"scanRowCount":0,"scanBytes":0,"scanFiles":0,"cuboidId":0}