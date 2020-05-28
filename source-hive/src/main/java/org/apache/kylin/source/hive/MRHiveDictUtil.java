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

package org.apache.kylin.source.hive;

import org.apache.kylin.shaded.com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.livy.LivyRestBuilder;
import org.apache.kylin.common.livy.LivyRestExecutor;
import org.apache.kylin.common.livy.LivyTypeEnum;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.job.JoinedFlatTable;
import org.apache.kylin.job.common.PatternedLogger;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * <pre>
 * Hold some constant/enum/statement for Hive Global Dictionary.
 *
 * There are two different temporary tables which help to build Hive Global Dictionary.
 * They should be deleted at the final step of building job.
 *   1. Distinct Value Table (Temporary table)
 *      TableName: ${FlatTable}_${DistinctValueSuffix}
 *      Schema: One normal column, for original column value; with another partition column.
 *   @see #distinctValueTable
 *
 *   2. Segment Level Dictionary Table (Temporary table)
 *      TableName: ${FlatTable}_${DictTableSuffix}
 *      Schema: Two normal columns, first for original column value, second for is its encoded integer;
 *          also with another partition column
 *   @see #segmentLevelDictTableName
 *
 * After that, Hive Global Dictionary itself is stored in a third hive table.
 *   3. Hive Global Dictionary Table
 *      TableName: ${CubeName}_${DictTableSuffix}
 *      Schema: Two columns, first for original column value, second is its encoded integer; also with another partition column
 *   @see #globalDictTableName
 * </pre>
 */
public class MRHiveDictUtil {
    private static final Logger logger = LoggerFactory.getLogger(MRHiveDictUtil.class);
    protected static final Pattern HDFS_LOCATION = Pattern.compile("LOCATION \'(.*)\';");

    public enum DictHiveType {
        GroupBy("group_by"), MrDictLockPath("/mr_dict_lock/"), MrEphemeralDictLockPath(
                "/mr_dict_ephemeral_lock/");
        private String name;

        DictHiveType(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    public static String distinctValueTable(IJoinedFlatTableDesc flatDesc) {
        return flatDesc.getTableName() + flatDesc.getSegment().getConfig().getMrHiveDistinctValueTableSuffix();
    }

    public static String segmentLevelDictTableName(IJoinedFlatTableDesc flatDesc) {
        return flatDesc.getTableName() + flatDesc.getSegment().getConfig().getMrHiveDictTableSuffix();
    }

    public static String globalDictTableName(IJoinedFlatTableDesc flatDesc, String cubeName) {
        return cubeName + flatDesc.getSegment().getConfig().getMrHiveDictTableSuffix();
    }

    public static String generateDictionaryDdl(String db, String tbl) {
        return "CREATE TABLE IF NOT EXISTS " + db + "." + tbl + "\n"
                + " ( dict_key STRING COMMENT '', \n"
                + "   dict_val INT COMMENT '' \n"
                + ") \n"
                + "COMMENT 'Hive Global Dictionary' \n"
                + "PARTITIONED BY (dict_column string) \n"
                + "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' \n"
                + "STORED AS TEXTFILE; \n";
    }

    public static String generateDropTableStatement(String tableName) {
        StringBuilder ddl = new StringBuilder();
        ddl.append("DROP TABLE IF EXISTS " + tableName + ";").append(" \n");
        return ddl.toString();
    }

    public static String generateDistinctValueTableStatement(IJoinedFlatTableDesc flatDesc) {
        StringBuilder ddl = new StringBuilder();
        String table = flatDesc.getTableName()
                + flatDesc.getSegment().getConfig().getMrHiveDistinctValueTableSuffix();

        ddl.append("CREATE TABLE IF NOT EXISTS " + table + " \n");
        ddl.append("( \n ");
        ddl.append("  dict_key" + " " + "STRING" + " COMMENT '' \n");
        ddl.append(") \n");
        ddl.append("COMMENT '' \n");
        ddl.append("PARTITIONED BY (dict_column string) \n");
        ddl.append("STORED AS TEXTFILE \n");
        ddl.append(";").append("\n");
        return ddl.toString();
    }

    public static String generateDictTableStatement(String globalTableName) {
        StringBuilder ddl = new StringBuilder();

        ddl.append("CREATE TABLE IF NOT EXISTS " + globalTableName + " \n");
        ddl.append("( \n ");
        ddl.append("  dict_key" + " " + "STRING" + " COMMENT '' , \n");
        ddl.append("  dict_val" + " " + "STRING" + " COMMENT '' \n");
        ddl.append(") \n");
        ddl.append("COMMENT '' \n");
        ddl.append("PARTITIONED BY (dict_column string) \n");
        ddl.append("ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' \n");
        ddl.append("STORED AS TEXTFILE \n");
        ddl.append(";").append("\n");
        return ddl.toString();
    }

    /**
     * Fetch distinct value from flat table and insert into distinctValueTable.
     *
     * @see #distinctValueTable
     */
    public static String generateInsertDataStatement(IJoinedFlatTableDesc flatDesc, String dictColumn, String globalDictDatabase, String globalDictTable) {
        int index = 0;
        for (TblColRef tblColRef : flatDesc.getAllColumns()) {
            if (JoinedFlatTable.colName(tblColRef, flatDesc.useAlias()).equalsIgnoreCase(dictColumn)) {
                break;
            }
            index++;
        }
        if (index == flatDesc.getAllColumns().size()) {
            String msg = "Can not find correct column for " + dictColumn
                    + ", please check 'kylin.dictionary.mr-hive.columns'";
            logger.error(msg);
            throw new IllegalArgumentException(msg);
        }

        String table = distinctValueTable(flatDesc);
        StringBuilder sql = new StringBuilder();
        TblColRef col = flatDesc.getAllColumns().get(index);

        sql.append("SELECT a.DICT_KEY FROM (\n");
        sql.append("  SELECT " + "\n");
        sql.append(JoinedFlatTable.colName(col)).append(" as DICT_KEY \n");
        sql.append("  FROM ").append(flatDesc.getTableName()).append("\n");
        sql.append("  GROUP BY ");
        sql.append(JoinedFlatTable.colName(col)).append(") a \n");
        sql.append("    LEFT JOIN \n");
        sql.append("  (SELECT DICT_KEY FROM ").append(globalDictDatabase).append(".").append(globalDictTable);
        sql.append("    WHERE DICT_COLUMN = '").append(dictColumn);
        sql.append("' ) b \n");
        sql.append("ON a.DICT_KEY = b.DICT_KEY \n");
        sql.append("WHERE b.DICT_KEY IS NULL \n");

        return "INSERT OVERWRITE TABLE " + table + " \n"
                + "PARTITION (dict_column = '" + dictColumn + "')" + " \n"
                + sql.toString()
                + ";\n";
    }

    /**
     * Calculate and store "columnName,segmentDistinctCount,previousMaxDictId" into specific partition
     */
    public static String generateDictStatisticsSql(String distinctValueTable, String globalDictTable, String globalDictDatabase) {
        return "INSERT OVERWRITE TABLE  " + distinctValueTable + " PARTITION (DICT_COLUMN = '" + BatchConstants.CFG_GLOBAL_DICT_STATS_PARTITION_VALUE + "') "
                + "\n" + "SELECT CONCAT_WS(',', tc.dict_column, cast(tc.total_distinct_val AS String), if(tm.max_dict_val is null, '0', cast(max_dict_val as string))) "
                + "\n" + "FROM ("
                + "\n" + "    SELECT dict_column, count(1) total_distinct_val"
                + "\n" + "    FROM " + globalDictDatabase + "." + distinctValueTable
                + "\n" + "    WHERE DICT_COLUMN != '" + BatchConstants.CFG_GLOBAL_DICT_STATS_PARTITION_VALUE + "'"
                + "\n" + "    GROUP BY dict_column) tc "
                + "\n" + "LEFT JOIN (\n"
                + "\n" + "    SELECT dict_column, if(max(dict_val) is null, 0, max(dict_val)) as max_dict_val "
                + "\n" + "    FROM " + globalDictDatabase + "." + globalDictTable
                + "\n" + "    GROUP BY dict_column) tm "
                + "\n" + "ON tc.dict_column = tm.dict_column;";
    }

    public static void runLivySqlJob(PatternedLogger stepLogger, KylinConfig config, ImmutableList<String> sqls,
                                     ExecutableManager executableManager, String jobId) throws IOException {
        final LivyRestBuilder livyRestBuilder = new LivyRestBuilder();
        livyRestBuilder.overwriteHiveProps(config.getHiveConfigOverride());
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(livyRestBuilder.parseProps());
        for (String sql : sqls) {
            stringBuilder.append(sql);
        }
        String args = stringBuilder.toString();
        livyRestBuilder.addArgs(args);

        stepLogger.log("Create and distribute table. ");
        livyRestBuilder.setLivyTypeEnum(LivyTypeEnum.sql);

        LivyRestExecutor executor = new LivyRestExecutor();
        executor.execute(livyRestBuilder, stepLogger);

        Map<String, String> info = stepLogger.getInfo();
        // get the flat Hive table size
        Matcher matcher = HDFS_LOCATION.matcher(args);
        if (matcher.find()) {
            String hiveFlatTableHdfsUrl = matcher.group(1);
            long size = getFileSize(hiveFlatTableHdfsUrl);
            info.put(ExecutableConstants.HDFS_BYTES_WRITTEN, "" + size);
            logger.info("HDFS_Bytes_Writen: {}", size);
        }
        executableManager.addJobInfo(jobId, info);
    }

    public static String getLockPath(String cubeName, String jobId) {
        if (jobId == null) {
            return DictHiveType.MrDictLockPath.getName() + cubeName;
        } else {
            return DictHiveType.MrDictLockPath.getName() + cubeName + "/" + jobId;
        }
    }

    public static String getEphemeralLockPath(String cubeName) {
        return DictHiveType.MrEphemeralDictLockPath.getName() + cubeName;
    }

    private static long getFileSize(String hdfsUrl) throws IOException {
        Configuration configuration = new Configuration();
        Path path = new Path(hdfsUrl);
        FileSystem fs = path.getFileSystem(configuration);
        ContentSummary contentSummary = fs.getContentSummary(path);
        return contentSummary.getLength();
    }
}
