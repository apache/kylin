SELECT lstg_format_name 
FROM   test_kylin_fact 
WHERE  ( NOT ( ( CASE 
                   WHEN ( lstg_format_name IS NULL ) THEN 1
                   WHEN NOT ( lstg_format_name IS NULL ) THEN 0 
                   ELSE NULL 
                 END ) <> 0 ) ) 
GROUP  BY lstg_format_name 