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

package org.apache.kylin.metadata.streaming;

import static org.mybatis.dynamic.sql.SqlBuilder.isEqualTo;
import static org.mybatis.dynamic.sql.SqlBuilder.isLessThan;
import static org.mybatis.dynamic.sql.SqlBuilder.select;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.ibatis.jdbc.ScriptRunner;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.logging.LogOutputStream;
import org.apache.kylin.common.persistence.metadata.JdbcDataSource;
import org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil;
import org.apache.kylin.metadata.streaming.util.StreamingJobRecordStoreUtil;
import org.mybatis.dynamic.sql.BasicColumn;
import org.mybatis.dynamic.sql.SqlBuilder;
import org.mybatis.dynamic.sql.delete.render.DeleteStatementProvider;
import org.mybatis.dynamic.sql.insert.render.InsertStatementProvider;
import org.mybatis.dynamic.sql.render.RenderingStrategies;
import org.mybatis.dynamic.sql.select.render.SelectStatementProvider;

import com.google.common.annotations.VisibleForTesting;

import lombok.Getter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JdbcStreamingJobRecordStore {

    private static final Charset DEFAULT_CHARSET = Charset.defaultCharset();
    public static final String TOTAL_ROW_COUNT = "count";

    private final StreamingJobRecordTable table;

    @VisibleForTesting
    @Getter
    private final SqlSessionFactory sqlSessionFactory;
    private final DataSource dataSource;
    String tableName;

    public JdbcStreamingJobRecordStore(KylinConfig config) throws Exception {
        StorageURL url = config.getStreamingStatsUrl();
        Properties props = JdbcUtil.datasourceParameters(url);
        dataSource = JdbcDataSource.getDataSource(props);
        tableName = StorageURL.replaceUrl(url) + "_" + StreamingJobRecord.STREAMING_JOB_RECORD_SUFFIX;
        table = new StreamingJobRecordTable(tableName);
        sqlSessionFactory = StreamingJobRecordStoreUtil.getSqlSessionFactory(dataSource, tableName);
    }

    public List<StreamingJobRecord> queryByJobId(String jobId) {
        try (SqlSession session = sqlSessionFactory.openSession()) {
            val mapper = session.getMapper(StreamingJobRecordMapper.class);
            SelectStatementProvider statementProvider = getSelectByJobIdStatementProvider(-1, jobId);
            return mapper.selectMany(statementProvider);
        }
    }

    public StreamingJobRecord getLatestOneByJobId(String jobId) {
        try (SqlSession session = sqlSessionFactory.openSession()) {
            val mapper = session.getMapper(StreamingJobRecordMapper.class);
            SelectStatementProvider statementProvider = getSelectByJobIdStatementProvider(1, jobId);
            return mapper.selectOne(statementProvider);
        }
    }

    public void dropTable() throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            ScriptRunner sr = new ScriptRunner(connection);
            sr.setLogWriter(new PrintWriter(new OutputStreamWriter(new LogOutputStream(log), DEFAULT_CHARSET)));
            sr.runScript(new InputStreamReader(
                    new ByteArrayInputStream(
                            String.format(Locale.ROOT, "drop table %s;", tableName).getBytes(DEFAULT_CHARSET)),
                    DEFAULT_CHARSET));
        }
    }

    public int insert(StreamingJobRecord streamingJobRecord) {
        try (SqlSession session = sqlSessionFactory.openSession()) {
            StreamingJobRecordMapper mapper = session.getMapper(StreamingJobRecordMapper.class);
            InsertStatementProvider<StreamingJobRecord> insertStatement = getInsertProvider(streamingJobRecord);
            int rows = mapper.insert(insertStatement);
            if (rows > 0) {
                log.debug("Insert one streaming job record(job id:{}, time:{}) into database.",
                        streamingJobRecord.getJobId(), streamingJobRecord.getCreateTime());
            }
            session.commit();
            return rows;
        }
    }

    public void deleteStreamingJobRecord(long timeline) {
        long startTime = System.currentTimeMillis();
        try (SqlSession session = sqlSessionFactory.openSession()) {
            val mapper = session.getMapper(StreamingJobRecordMapper.class);
            DeleteStatementProvider deleteStatement;
            if (timeline < 0) {
                deleteStatement = SqlBuilder.deleteFrom(table) //
                        .build().render(RenderingStrategies.MYBATIS3);
            } else {
                deleteStatement = SqlBuilder.deleteFrom(table) //
                        .where(table.createTime, isLessThan(timeline)) //
                        .build().render(RenderingStrategies.MYBATIS3);
            }
            int deleteRows = mapper.delete(deleteStatement);
            session.commit();
            if (deleteRows > 0) {
                log.info("Delete {} row streaming job stats takes {} ms", deleteRows,
                        System.currentTimeMillis() - startTime);
            }
        }
    }

    SelectStatementProvider getSelectByJobIdStatementProvider(int limitSize, String jobId) {
        val builder = select(getSelectFields(table)) //
                .from(table) //
                .where(table.jobId, isEqualTo(jobId)); //
        if (limitSize > 0) {
            return builder.orderBy(table.createTime.descending()).limit(limitSize) //
                    .build().render(RenderingStrategies.MYBATIS3);
        } else {
            return builder.orderBy(table.createTime.descending()) //
                    .build().render(RenderingStrategies.MYBATIS3);
        }
    }

    InsertStatementProvider<StreamingJobRecord> getInsertProvider(StreamingJobRecord record) {
        return SqlBuilder.insert(record).into(table).map(table.id).toPropertyWhenPresent("id", record::getId) //
                .map(table.jobId).toPropertyWhenPresent("jobId", record::getJobId) //
                .map(table.project).toPropertyWhenPresent("project", record::getProject) //
                .map(table.action).toPropertyWhenPresent("action", record::getAction) //
                .map(table.createTime).toPropertyWhenPresent("createTime", record::getCreateTime) //
                .map(table.updateTime).toPropertyWhenPresent("updateTime", record::getUpdateTime) //
                .build().render(RenderingStrategies.MYBATIS3);
    }

    private BasicColumn[] getSelectFields(StreamingJobRecordTable recordTable) {
        return BasicColumn.columnList(//
                recordTable.id, //
                recordTable.jobId, //
                recordTable.project, //
                recordTable.action, //
                recordTable.createTime, //
                recordTable.updateTime);
    }

}
