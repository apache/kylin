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

public class MRHiveDictUtil {
    private static final Logger logger = LoggerFactory.getLogger(MRHiveDictUtil.class);
    protected static final Pattern HDFS_LOCATION = Pattern.compile("LOCATION \'(.*)\';");

    public enum DictHiveType {
        GroupBy("group_by"), MrDictLockPath("/mr_dict_lock/");
        private String name;

        DictHiveType(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    public static String generateDropTableStatement(IJoinedFlatTableDesc flatDesc) {
        StringBuilder ddl = new StringBuilder();
        String table = getHiveTableName(flatDesc, DictHiveType.GroupBy);
        ddl.append("DROP TABLE IF EXISTS " + table + ";").append(" \n");
        return ddl.toString();
    }

    public static String generateCreateTableStatement(IJoinedFlatTableDesc flatDesc) {
        StringBuilder ddl = new StringBuilder();
        String table = getHiveTableName(flatDesc, DictHiveType.GroupBy);

        ddl.append("CREATE TABLE IF NOT EXISTS " + table + " \n");
        ddl.append("( \n ");
        ddl.append("dict_key" + " " + "STRING" + " COMMENT '' \n");
        ddl.append(") \n");
        ddl.append("COMMENT '' \n");
        ddl.append("PARTITIONED BY (dict_column string) \n");
        ddl.append("STORED AS SEQUENCEFILE \n");
        ddl.append(";").append("\n");
        return ddl.toString();
    }

    public static String generateInsertDataStatement(IJoinedFlatTableDesc flatDesc, String dictColumn) {
        String table = getHiveTableName(flatDesc, DictHiveType.GroupBy);

        StringBuilder sql = new StringBuilder();
        sql.append("SELECT" + "\n");

        int index = 0;
        for (TblColRef tblColRef : flatDesc.getAllColumns()) {
            if (JoinedFlatTable.colName(tblColRef, flatDesc.useAlias()).equalsIgnoreCase(dictColumn)) {
                break;
            }
            index++;
        }

        if (index == flatDesc.getAllColumns().size()) {
            String msg = "Can not find correct column for " + dictColumn + ", please check 'kylin.dictionary.mr-hive.columns'";
            logger.error(msg);
            throw new IllegalArgumentException(msg);
        }

        TblColRef col = flatDesc.getAllColumns().get(index);
        sql.append(JoinedFlatTable.colName(col) + " \n");

        MRHiveDictUtil.appendJoinStatement(flatDesc, sql);

        //group by
        sql.append("GROUP BY ");
        sql.append(JoinedFlatTable.colName(col) + " \n");

        return "INSERT OVERWRITE TABLE " + table + " \n"
                + "PARTITION (dict_column = '" + dictColumn + "')" + " \n"
                + sql + ";\n";
    }

    public static String getHiveTableName(IJoinedFlatTableDesc flatDesc, DictHiveType dictHiveType) {
        StringBuffer table = new StringBuffer(flatDesc.getTableName());
        table.append("__");
        table.append(dictHiveType.getName());
        return table.toString();
    }

    public static void appendJoinStatement(IJoinedFlatTableDesc flatDesc, StringBuilder sql) {
        sql.append("FROM " + flatDesc.getTableName() + "\n");
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
        //get the flat Hive table size
        Matcher matcher = HDFS_LOCATION.matcher(args);
        if (matcher.find()) {
            String hiveFlatTableHdfsUrl = matcher.group(1);
            long size = getFileSize(hiveFlatTableHdfsUrl);
            info.put(ExecutableConstants.HDFS_BYTES_WRITTEN, "" + size);
            logger.info("HDFS_Bytes_Writen: {}", size);
        }
        executableManager.addJobInfo(jobId, info);
    }

    private static long getFileSize(String hdfsUrl) throws IOException {
        Configuration configuration = new Configuration();
        Path path = new Path(hdfsUrl);
        FileSystem fs = path.getFileSystem(configuration);
        ContentSummary contentSummary = fs.getContentSummary(path);
        return contentSummary.getLength();
    }
}
