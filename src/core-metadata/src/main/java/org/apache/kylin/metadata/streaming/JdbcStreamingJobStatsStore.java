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
import static org.mybatis.dynamic.sql.SqlBuilder.isGreaterThan;
import static org.mybatis.dynamic.sql.SqlBuilder.isGreaterThanOrEqualTo;
import static org.mybatis.dynamic.sql.SqlBuilder.isLessThan;
import static org.mybatis.dynamic.sql.SqlBuilder.max;
import static org.mybatis.dynamic.sql.SqlBuilder.min;
import static org.mybatis.dynamic.sql.SqlBuilder.select;
import static org.mybatis.dynamic.sql.SqlBuilder.sum;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.apache.ibatis.jdbc.ScriptRunner;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.logging.LogOutputStream;
import org.apache.kylin.common.persistence.metadata.JdbcDataSource;
import org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil;
import org.apache.kylin.metadata.streaming.util.StreamingJobStatsStoreUtil;
import org.mybatis.dynamic.sql.BasicColumn;
import org.mybatis.dynamic.sql.SqlBuilder;
import org.mybatis.dynamic.sql.delete.render.DeleteStatementProvider;
import org.mybatis.dynamic.sql.insert.render.InsertStatementProvider;
import org.mybatis.dynamic.sql.render.RenderingStrategies;
import org.mybatis.dynamic.sql.select.render.SelectStatementProvider;

import org.apache.kylin.guava30.shaded.common.annotations.VisibleForTesting;
import org.apache.kylin.guava30.shaded.common.collect.Lists;

import lombok.Getter;
import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JdbcStreamingJobStatsStore {

    private static final Charset DEFAULT_CHARSET = Charset.defaultCharset();
    public static final String TOTAL_ROW_COUNT = "count";
    public static final String MIN_RATE = "min_rate";
    public static final String MAX_RATE = "max_rate";

    private final StreamingJobStatsTable streamingJobStatsTable;

    @VisibleForTesting
    @Getter
    private final SqlSessionFactory sqlSessionFactory;
    private final DataSource dataSource;
    String tableName;

    public JdbcStreamingJobStatsStore(KylinConfig config) throws Exception {
        StorageURL url = config.getStreamingStatsUrl();
        Properties props = JdbcUtil.datasourceParameters(url);
        dataSource = JdbcDataSource.getDataSource(props);
        tableName = StorageURL.replaceUrl(url) + "_" + StreamingJobStats.STREAMING_JOB_STATS_SUFFIX;
        streamingJobStatsTable = new StreamingJobStatsTable(tableName);
        sqlSessionFactory = StreamingJobStatsStoreUtil.getSqlSessionFactory(dataSource, tableName);
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

    public int insert(StreamingJobStats streamingJobStats) {
        try (SqlSession session = sqlSessionFactory.openSession()) {
            StreamingJobStatsMapper sjsMapper = session.getMapper(StreamingJobStatsMapper.class);
            InsertStatementProvider<StreamingJobStats> insertStatement = getInsertSJSProvider(streamingJobStats);
            int rows = sjsMapper.insert(insertStatement);
            if (rows > 0) {
                log.debug("Insert one streaming job stats(job id:{}, time:{}) into database.",
                        streamingJobStats.getJobId(), streamingJobStats.getCreateTime());
            }
            session.commit();
            return rows;
        }
    }

    public void insert(List<StreamingJobStats> statsList) {
        long startTime = System.currentTimeMillis();
        try (SqlSession session = sqlSessionFactory.openSession(ExecutorType.BATCH)) {
            StreamingJobStatsMapper mapper = session.getMapper(StreamingJobStatsMapper.class);
            List<InsertStatementProvider<StreamingJobStats>> providers = Lists.newArrayList();
            statsList.forEach(stats -> providers.add(getInsertSJSProvider(stats)));
            providers.forEach(mapper::insert);
            session.commit();
            if (statsList.size() > 0) {
                log.info("Insert {} streaming job stats into database takes {} ms", statsList.size(),
                        System.currentTimeMillis() - startTime);
            }
        }
    }

    public StreamingJobStats getLatestOneByJobId(String jobId) {
        try (SqlSession session = sqlSessionFactory.openSession()) {
            val mapper = session.getMapper(StreamingJobStatsMapper.class);
            SelectStatementProvider statementProvider = getSelectByJobIdStatementProvider(-1, jobId);
            return mapper.selectOne(statementProvider);
        }
    }

    public Map<String, Long> queryDataLatenciesByJobIds(List<String> jobIds) {
        try (SqlSession session = sqlSessionFactory.openSession()) {
            val mapper = session.getMapper(StreamingJobStatsMapper.class);
            SelectStatementProvider statementProvider = queryLatestJobIdProvider(jobIds);
            val idList = mapper.selectLatestJobId(statementProvider).stream().map(item -> item.getValue())
                    .collect(Collectors.toList());
            val list = mapper.selectMany(getSelectByIdsStatementProvider(idList));
            val resultMap = new HashMap<String, Long>();
            list.stream().forEach(item -> resultMap.put(item.getJobId(), item.getMinDataLatency()));
            return resultMap;
        }
    }

    public List<StreamingJobStats> queryByJobId(long startTime, String jobId) {
        try (SqlSession session = sqlSessionFactory.openSession()) {
            val mapper = session.getMapper(StreamingJobStatsMapper.class);
            SelectStatementProvider statementProvider = getSelectByJobIdStatementProvider(startTime, jobId);
            return mapper.selectMany(statementProvider);
        }
    }

    public ConsumptionRateStats queryAvgConsumptionRate(long startTime, String jobId) {
        try (SqlSession session = sqlSessionFactory.openSession()) {
            val mapper = session.getMapper(StreamingJobStatsMapper.class);
            SelectStatementProvider statementProvider = queryAllTimeAvgConsumerRate(startTime, jobId);
            return mapper.selectStreamingStatistics(statementProvider);
        }
    }

    public List<RowCountDetailByTime> queryRowCountDetailByTime(long startTime, String jobId) {
        try (SqlSession session = sqlSessionFactory.openSession()) {
            RowCountDetailByTimeMapper mapper = session.getMapper(RowCountDetailByTimeMapper.class);
            SelectStatementProvider statementProvider = queryAvgConsumerRateByTime(jobId, startTime);
            return mapper.selectMany(statementProvider);
        }
    }

    public void deleteStreamingJobStats(long timeline) {
        long startTime = System.currentTimeMillis();
        try (SqlSession session = sqlSessionFactory.openSession()) {
            StreamingJobStatsMapper mapper = session.getMapper(StreamingJobStatsMapper.class);

            DeleteStatementProvider deleteStatement;
            if (timeline < 0) {
                deleteStatement = SqlBuilder.deleteFrom(streamingJobStatsTable) //
                        .build().render(RenderingStrategies.MYBATIS3);
            } else {
                deleteStatement = SqlBuilder.deleteFrom(streamingJobStatsTable) //
                        .where(streamingJobStatsTable.createTime, isLessThan(timeline)) //
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

    InsertStatementProvider<StreamingJobStats> getInsertSJSProvider(StreamingJobStats stats) {
        return SqlBuilder.insert(stats).into(streamingJobStatsTable).map(streamingJobStatsTable.jobId)
                .toPropertyWhenPresent("jobId", stats::getJobId) //
                .map(streamingJobStatsTable.projectName).toPropertyWhenPresent("projectName", stats::getProjectName) //
                .map(streamingJobStatsTable.batchRowNum).toPropertyWhenPresent("batchRowNum", stats::getBatchRowNum) //
                .map(streamingJobStatsTable.rowsPerSecond)
                .toPropertyWhenPresent("rowsPerSecond", stats::getRowsPerSecond) //
                .map(streamingJobStatsTable.createTime).toPropertyWhenPresent("createTime", stats::getCreateTime) //
                .map(streamingJobStatsTable.processingTime)
                .toPropertyWhenPresent("processingTime", stats::getProcessingTime) //
                .map(streamingJobStatsTable.minDataLatency)
                .toPropertyWhenPresent("minDataLatency", stats::getMinDataLatency) //
                .map(streamingJobStatsTable.maxDataLatency)
                .toPropertyWhenPresent("maxDataLatency", stats::getMaxDataLatency) //
                .build().render(RenderingStrategies.MYBATIS3);
    }

    private SelectStatementProvider queryAllTimeAvgConsumerRate(long startTime, String jobId) {
        return select(min(streamingJobStatsTable.rowsPerSecond).as(MIN_RATE),
                max(streamingJobStatsTable.rowsPerSecond).as(MAX_RATE),
                sum(streamingJobStatsTable.batchRowNum).as(TOTAL_ROW_COUNT)).from(streamingJobStatsTable)
                        .where(streamingJobStatsTable.createTime, isGreaterThanOrEqualTo(startTime))
                        .and(streamingJobStatsTable.jobId, isEqualTo(jobId)).build()
                        .render(RenderingStrategies.MYBATIS3);
    }

    private SelectStatementProvider queryAvgConsumerRateByTime(String jobId, long startTime) {
        return select(getSelectFields(streamingJobStatsTable)).from(streamingJobStatsTable)
                .where(streamingJobStatsTable.createTime, isGreaterThanOrEqualTo(getLastHourRetainTime()))
                .and(streamingJobStatsTable.jobId, isEqualTo(jobId))
                .and(streamingJobStatsTable.batchRowNum, isGreaterThan(0L))
                .and(streamingJobStatsTable.createTime, isGreaterThanOrEqualTo(startTime))
                .orderBy(streamingJobStatsTable.createTime.descending()).build().render(RenderingStrategies.MYBATIS3);
    }

    private long getLastHourRetainTime() {
        return new Date(System.currentTimeMillis() - 60 * 60 * 1000).getTime();
    }

    SelectStatementProvider queryLatestJobIdProvider(List<String> jobIds) {
        return select(streamingJobStatsTable.jobId, max(streamingJobStatsTable.id).as("id"))
                .from(streamingJobStatsTable).where(streamingJobStatsTable.jobId, SqlBuilder.isIn(jobIds))
                .groupBy(BasicColumn.columnList(streamingJobStatsTable.jobId)).build()
                .render(RenderingStrategies.MYBATIS3);
    }

    SelectStatementProvider getSelectByJobIdStatementProvider(long startTime, String jobId) {
        var builder = select(getSelectAllFields(streamingJobStatsTable)) //
                .from(streamingJobStatsTable) //
                .where(streamingJobStatsTable.jobId, isEqualTo(jobId)); //
        if (startTime > 0) {
            builder = builder.and(streamingJobStatsTable.createTime, isGreaterThanOrEqualTo(startTime));
            return builder.orderBy(streamingJobStatsTable.createTime.descending()).build()
                    .render(RenderingStrategies.MYBATIS3);
        }
        return builder.orderBy(streamingJobStatsTable.createTime.descending()).limit(1).build()
                .render(RenderingStrategies.MYBATIS3);

    }

    SelectStatementProvider getSelectByIdsStatementProvider(List<Long> ids) {
        return select(getSelectAllFields(streamingJobStatsTable)) //
                .from(streamingJobStatsTable) //
                .where(streamingJobStatsTable.id, SqlBuilder.isIn(ids))
                .orderBy(streamingJobStatsTable.createTime.descending()).build().render(RenderingStrategies.MYBATIS3);

    }

    private BasicColumn[] getSelectAllFields(StreamingJobStatsTable statsTable) {
        return BasicColumn.columnList(//
                statsTable.id, //
                statsTable.jobId, //
                statsTable.projectName, //
                statsTable.batchRowNum, //
                statsTable.rowsPerSecond, //
                statsTable.processingTime, //
                statsTable.minDataLatency, //
                statsTable.maxDataLatency, //
                statsTable.createTime);
    }

    private BasicColumn[] getSelectFields(StreamingJobStatsTable streamingJobStatsTable) {
        return BasicColumn.columnList(streamingJobStatsTable.createTime, streamingJobStatsTable.batchRowNum);
    }
}
