### Quick start with sample cube

Kylin provides a script for you to create a sample Cube; the script will also create three sample hive tables:

1. Run ${KYLIN_HOME}/bin/sample.sh ; Restart kylin server to flush the caches;
2. Logon Kylin web, select project "learn_kylin";
3. Select the sample cube "kylin_sales_cube", click "Actions" -> "Build", pick up a date later than 2014-01-01 (to cover all 10000 sample records);
4. Check the build progress in "Jobs" tab, until 100%;
5. Execute SQLs in the "Query" tab, for example:
	select cal_dt, sum(price) as total_selled, count(distinct seller_id) as sellers from kylin_sales group by cal_dt order by cal_dt
6. You can verify the query result and compare the response time with hive;

   
#### What's next

After cube being built, you might need to:

1. [Query the cube via web interface](Kylin Web Tutorial.md)
2. [Query the cube via ODBC](Kylin ODBC Driver Tutorial.md)
3. [Grant permission to cubes](Kylin Cube Permission Grant Tutorial.md)