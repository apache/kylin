/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.apache.kylin.monitor;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.log4j.Logger;
import org.datanucleus.util.StringUtils;

/**
 * Created by jiazhong on 2015/6/17.
 */
public class HiveJdbcClient {

    static String SQL_GENERATE_QUERY_LOG_TABLE = "CREATE EXTERNAL TABLE IF NOT EXISTS [QUERY_LOG_TABLE_NAME] (REQUEST_TIME STRING,REQUEST_DATE DATE, QUERY_SQL STRING,QUERY_USER STRING,IS_SUCCESS STRING,QUERY_LATENCY DECIMAL(19,4),QUERY_PROJECT STRING,REALIZATION_NAMES STRING,CUBOID_IDS STRING,TOTAL_SCAN_COUNT INT,RESULT_ROW_COUNT INT,ACCEPT_PARTIAL STRING,IS_PARTIAL_RESULT STRING,HIT_CACHE STRING,MESSAGE STRING,DEPLOY_ENV STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' LINES TERMINATED BY '\\n' LOCATION '[QUERY_LOG_PARSE_RESULT_DIR]' TBLPROPERTIES (\"SKIP.HEADER.LINE.COUNT\"=\"1\")";

    static String SQL_TOTAL_QUERY_USER = "SELECT  COUNT(DISTINCT QUERY_USER) FROM [QUERY_LOG_TABLE_NAME]";

    static String SQL_AVG_DAY_QUERY = "SELECT AVG(A.COUNT_QUERY) FROM (SELECT COUNT(*) COUNT_QUERY,REQUEST_DATE FROM  [QUERY_LOG_TABLE_NAME] GROUP BY REQUEST_DATE) A";

    static String SQL_LAST_30_DAYILY_QUERY_COUNT = "SELECT REQUEST_DATE, COUNT(*) FROM  [QUERY_LOG_TABLE_NAME]  WHERE  REQUEST_DATE>=[START_DATE] AND REQUEST_DATE<[END_DATE]  GROUP BY REQUEST_DATE";

    //last 30 days
    static String SQL_90_PERCENTTILE_LAST_30_DAY = "SELECT PERCENTILE_APPROX(LOG.QUERY_LATENCY,0.9) FROM (SELECT QUERY_LATENCY FROM [QUERY_LOG_TABLE_NAME]  WHERE IS_SUCCESS='true' AND REQUEST_DATE>=[START_DATE] AND REQUEST_DATE<[END_DATE]) LOG";

    //0.9,0.95 [each day] percentile in last 30 days
    static String SQL_EACH_DAY_PERCENTILE = "SELECT REQUEST_DATE, PERCENTILE_APPROX(QUERY_LATENCY,ARRAY(0.9,0.95)),COUNT(*) FROM  [QUERY_LOG_TABLE_NAME]  WHERE IS_SUCCESS='true' AND REQUEST_DATE>=[START_DATE] AND REQUEST_DATE<[END_DATE]  GROUP BY REQUEST_DATE";

    //0.9,0.95 [project] percentile in last 30 days
    static String SQL_DAY_PERCENTILE_BY_PROJECT = "SELECT QUERY_PROJECT, PERCENTILE_APPROX(QUERY_LATENCY,ARRAY(0.9,0.95)) FROM  [QUERY_LOG_TABLE_NAME]  WHERE IS_SUCCESS='true' AND  REQUEST_DATE>=[START_DATE] AND REQUEST_DATE<[END_DATE]  GROUP BY QUERY_PROJECT";

    static String QUERY_LOG_TABLE_NAME = "KYLIN_QUERY_LOG";

    final static Logger logger = Logger.getLogger(HiveJdbcClient.class);

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static ConfigUtils monitorConfig = ConfigUtils.getInstance();

    static {
        try {
            Class.forName(driverName);
            monitorConfig.loadMonitorParam();
            QUERY_LOG_TABLE_NAME = monitorConfig.getQueryLogResultTable();
            if (StringUtils.isEmpty(QUERY_LOG_TABLE_NAME)) {
                logger.error("table name not defined ,please set param [query.log.parse.result.table] in kylin.properties");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /*
     * will create external hive table for [query] log parse result csv file on hdfs
     * and will generate metric data in csv file on hdfs for web dashboard
     */
    public void start() throws SQLException, IOException {

        String CON_URL = monitorConfig.getHiveJdbcConUrl();

        String USER_NAME = monitorConfig.getHiveJdbcConUserName();
        String PASSWD = monitorConfig.getHiveJdbcConPasswd();

        Connection con = DriverManager.getConnection(CON_URL, USER_NAME, PASSWD);
        Statement stmt = con.createStatement();
        ResultSet res = null;

        SQL_GENERATE_QUERY_LOG_TABLE = generateQueryLogSql();
        logger.info("Running Sql (Create Table):" + SQL_GENERATE_QUERY_LOG_TABLE);
        stmt.execute(SQL_GENERATE_QUERY_LOG_TABLE);

        SQL_TOTAL_QUERY_USER = generateUserCountSql();
        logger.info("Running Sql (Total User):" + SQL_TOTAL_QUERY_USER);
        res = stmt.executeQuery(SQL_TOTAL_QUERY_USER);

        String total_query_user_path = monitorConfig.getLogParseResultMetaDir() + "total_query_user.csv";
        FileUtils.pathCheck(total_query_user_path);
        FileUtils.clearHdfsFile(total_query_user_path);
        while (res.next()) {
            FileUtils.appendResultToHdfs(total_query_user_path, new String[] { res.getString(1) });
            logger.info("Total User:" + res.getString(1));
        }

        SQL_AVG_DAY_QUERY = generateAvgDayQuery();
        logger.info("Running Sql (Avg Day Query):" + SQL_AVG_DAY_QUERY);
        res = stmt.executeQuery(SQL_AVG_DAY_QUERY);

        String avg_day_query_path = monitorConfig.getLogParseResultMetaDir() + "avg_day_query.csv";
        FileUtils.pathCheck(avg_day_query_path);
        FileUtils.clearHdfsFile(avg_day_query_path);
        while (res.next()) {
            FileUtils.appendResultToHdfs(avg_day_query_path, new String[] { res.getString(1) });
            logger.info("avg day query:" + res.getString(1));
        }

        SQL_LAST_30_DAYILY_QUERY_COUNT = generateLast30DayilyQueryCount();
        logger.info("Running Sql (Daily Query Count):" + SQL_LAST_30_DAYILY_QUERY_COUNT);
        res = stmt.executeQuery(SQL_LAST_30_DAYILY_QUERY_COUNT);

        String last_30_daily_query_count_path = monitorConfig.getLogParseResultMetaDir() + "last_30_daily_query_count.csv";
        FileUtils.pathCheck(last_30_daily_query_count_path);
        FileUtils.clearHdfsFile(last_30_daily_query_count_path);
        while (res.next()) {
            FileUtils.appendResultToHdfs(last_30_daily_query_count_path, new String[] { res.getString(1), res.getString(2) });
            logger.info("last 30 daily query count:" + res.getString(1) + "," + res.getString(2));
        }

        //90 percentile latency for all query in last 30 days
        SQL_90_PERCENTTILE_LAST_30_DAY = generateNintyPercentileSql();
        logger.info("Running Sql (last 30 days ,90 percentile query latency):" + SQL_90_PERCENTTILE_LAST_30_DAY);
        res = stmt.executeQuery(SQL_90_PERCENTTILE_LAST_30_DAY);

        String last_30_day_90_percentile_latency = monitorConfig.getLogParseResultMetaDir() + "last_30_day_90_percentile_latency.csv";
        FileUtils.pathCheck(last_30_day_90_percentile_latency);
        FileUtils.clearHdfsFile(last_30_day_90_percentile_latency);
        while (res.next()) {
            FileUtils.appendResultToHdfs(last_30_day_90_percentile_latency, new String[] { res.getString(1) });
            logger.info("last 30 day 90 percentile latency:" + res.getString(1));
        }

        //90,95 project percentile latency for all query in last 30 days
        SQL_DAY_PERCENTILE_BY_PROJECT = generateProjectPercentileSql();
        logger.info("Running Sql (last 30 days ,90,95 percentile query latency by project):" + SQL_DAY_PERCENTILE_BY_PROJECT);
        res = stmt.executeQuery(SQL_DAY_PERCENTILE_BY_PROJECT);

        String last_30_day_project_percentile_latency_path = monitorConfig.getLogParseResultMetaDir() + "project_90_95_percentile_latency.csv";
        FileUtils.pathCheck(last_30_day_project_percentile_latency_path);
        FileUtils.clearHdfsFile(last_30_day_project_percentile_latency_path);
        while (res.next() && res.getMetaData().getColumnCount() == 2) {
            FileUtils.appendResultToHdfs(last_30_day_project_percentile_latency_path, new String[] { res.getString(1), res.getString(2) });
            logger.info(res.getString(1) + "," + res.getString(2));
        }

        //0.9,0.95 percentile latency of every day in last 30 day
        SQL_EACH_DAY_PERCENTILE = generateEachDayPercentileSql();
        logger.info("Running sql (0.9,0.95 latency):" + SQL_EACH_DAY_PERCENTILE);
        String each_day_percentile_file = monitorConfig.getLogParseResultMetaDir() + "each_day_90_95_percentile_latency.csv";
        FileUtils.pathCheck(each_day_percentile_file);
        FileUtils.clearHdfsFile(each_day_percentile_file);

        res = stmt.executeQuery(SQL_EACH_DAY_PERCENTILE);
        while (res.next() && res.getMetaData().getColumnCount() == 3) {
            FileUtils.appendResultToHdfs(each_day_percentile_file, new String[] { res.getString(1), res.getString(2), res.getString(3) });
            logger.info(res.getString(1) + "," + res.getString(2) + "," + res.getString(3));
        }

    }

    public String generateQueryLogSql() {
        String query_log_parse_result_dir = monitorConfig.getQueryLogParseResultDir();
        String query_log_table_name = monitorConfig.getQueryLogResultTable();
        return SQL_GENERATE_QUERY_LOG_TABLE.replace("[QUERY_LOG_PARSE_RESULT_DIR]", query_log_parse_result_dir).replace("[QUERY_LOG_TABLE_NAME]", query_log_table_name);
    }

    public String generateUserCountSql() {
        return SQL_TOTAL_QUERY_USER.replace("[QUERY_LOG_TABLE_NAME]", QUERY_LOG_TABLE_NAME);
    }

    public String generateAvgDayQuery() {
        return SQL_AVG_DAY_QUERY.replace("[QUERY_LOG_TABLE_NAME]", QUERY_LOG_TABLE_NAME);
    }

    public String generateLast30DayilyQueryCount() {
        SQL_LAST_30_DAYILY_QUERY_COUNT = SQL_LAST_30_DAYILY_QUERY_COUNT.replace("[QUERY_LOG_TABLE_NAME]", QUERY_LOG_TABLE_NAME);
        return monthStasticSqlConvert(SQL_LAST_30_DAYILY_QUERY_COUNT);
    }

    //last 30 days
    public String generateNintyPercentileSql() {
        SQL_90_PERCENTTILE_LAST_30_DAY = SQL_90_PERCENTTILE_LAST_30_DAY.replace("[QUERY_LOG_TABLE_NAME]", QUERY_LOG_TABLE_NAME);
        return monthStasticSqlConvert(SQL_90_PERCENTTILE_LAST_30_DAY);
    }

    //last 30 days,each day 90,95 percentile
    public String generateProjectPercentileSql() {
        SQL_DAY_PERCENTILE_BY_PROJECT = SQL_DAY_PERCENTILE_BY_PROJECT.replace("[QUERY_LOG_TABLE_NAME]", QUERY_LOG_TABLE_NAME);
        return monthStasticSqlConvert(SQL_DAY_PERCENTILE_BY_PROJECT);
    }

    public String generateEachDayPercentileSql() {
        SQL_EACH_DAY_PERCENTILE = SQL_EACH_DAY_PERCENTILE.replace("[QUERY_LOG_TABLE_NAME]", QUERY_LOG_TABLE_NAME);
        return monthStasticSqlConvert(SQL_EACH_DAY_PERCENTILE);
    }

    public String monthStasticSqlConvert(String sql) {

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, -1);
        String endDate = format.format(cal.getTime());
        cal.add(Calendar.DATE, -30);
        String startDate = format.format(cal.getTime());
        return sql.replace("[START_DATE]", "'" + startDate + "'").replace("[END_DATE]", "'" + endDate + "'");
    }

}