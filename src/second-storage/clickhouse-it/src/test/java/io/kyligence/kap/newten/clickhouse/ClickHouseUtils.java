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
package io.kyligence.kap.newten.clickhouse;

import static io.kyligence.kap.clickhouse.ClickHouseConstants.CONFIG_CLICKHOUSE_QUERY_CATALOG;
import static io.kyligence.kap.newten.clickhouse.SonarFixUtils.jdbcClassesArePresent;
import static io.kyligence.kap.secondstorage.SecondStorageConstants.CONFIG_SECOND_STORAGE_CLUSTER;
import static org.awaitility.Awaitility.await;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.engine.spark.IndexDataConstructor;
import org.apache.kylin.engine.spark.utils.RichOption;
import org.apache.kylin.job.SecondStorageJobParamUtil;
import org.apache.kylin.job.common.ExecutableUtil;
import org.apache.kylin.job.execution.DefaultExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.handler.AbstractJobHandler;
import org.apache.kylin.job.handler.SecondStorageSegmentLoadJobHandler;
import org.apache.kylin.job.model.JobParam;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.catalyst.plans.logical.Aggregate;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.ExplainMode;
import org.apache.spark.sql.execution.datasource.FilePruner;
import org.apache.spark.sql.execution.datasources.HadoopFsRelation;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation;
import org.apache.spark.sql.execution.datasources.v2.V1ScanWrapper;
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCScan;
import org.apache.spark.sql.execution.datasources.v2.jdbc.ShardJDBCTable;
import org.junit.Assert;
import org.testcontainers.containers.ClickHouseContainer;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableMap;

import io.kyligence.kap.clickhouse.ddl.ClickHouseCreateTable;
import io.kyligence.kap.clickhouse.ddl.ClickHouseRender;
import io.kyligence.kap.clickhouse.management.ClickHouseConfigLoader;
import io.kyligence.kap.secondstorage.SecondStorage;
import io.kyligence.kap.secondstorage.SecondStorageConstants;
import io.kyligence.kap.secondstorage.config.ClusterInfo;
import io.kyligence.kap.secondstorage.config.Node;
import io.kyligence.kap.secondstorage.ddl.InsertInto;
import io.kyligence.kap.secondstorage.ddl.exp.ColumnWithType;
import lombok.val;
import lombok.extern.slf4j.Slf4j;
import scala.runtime.AbstractFunction1;

@Slf4j
public class ClickHouseUtils {

    static final Network TEST_NETWORK = Network.newNetwork();
    private static final Pattern _extraQuotes = Pattern.compile("([\"]*)([^\"]*)([\"]*)");
    static public String DEFAULT_VERSION = "22.5.2.53";//"20.8.2.3"; //"20.10.3.30";"20.10.2.20";
    static public String DEFAULT_TAG = "clickhouse/clickhouse-server:" + DEFAULT_VERSION;
    static public DockerImageName CLICKHOUSE_IMAGE = DockerImageName.parse(DEFAULT_TAG)
            .asCompatibleSubstituteFor("yandex/clickhouse-server");

    static public JdbcDatabaseContainer<?> startClickHouse() {
        int tryTimes = 3;
        do {
            try {
                return internalStartClickHouse();
            } catch (Throwable e) {
                tryTimes--;
                if (tryTimes == 0) {
                    throw new RuntimeException("!!!can not start clickhouse docker!!!", e);
                }
            }
        } while (true);
    }

    private static JdbcDatabaseContainer<?> internalStartClickHouse() {
        JdbcDatabaseContainer<?> clickhouse = null;

        if (jdbcClassesArePresent("ru.yandex.clickhouse.ClickHouseDriver")) {
            clickhouse = new ClickHouseContainer(CLICKHOUSE_IMAGE);
        } else if (jdbcClassesArePresent("com.github.housepower.jdbc.ClickHouseDriver")) {
            clickhouse = new ClickHouseContainerWithNativeJDBC(CLICKHOUSE_IMAGE);
        }

        Assert.assertNotNull("Can not find JDBC Driver", clickhouse);

        try {
            clickhouse.withNetwork(TEST_NETWORK).start();
            //check clickhouse version
            final String url = clickhouse.getJdbcUrl();
            await().atMost(60, TimeUnit.SECONDS).until(() -> checkClickHouseAlive(url));
        } catch (Throwable r) {
            clickhouse.close();
            throw r;
        }
        return clickhouse;
    }

    private static boolean checkClickHouseAlive(String url) {
        try (Connection connection = DriverManager.getConnection(url);
                Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT version()")) {
            Assert.assertTrue(rs.next());
            String version = rs.getString(1);
            Assert.assertEquals(DEFAULT_VERSION, version);
            return true;
        } catch (SQLException s) {
            return false;
        }
    }

    // TODO: we need more general functional library
    @FunctionalInterface
    public interface CheckedFunction<T, R> {
        R apply(T t) throws Exception;
    }

    @FunctionalInterface
    public interface CheckedFunction2<T1, T2, R> {
        R apply(T1 t1, T2 t2) throws Exception;
    }

    @FunctionalInterface
    public interface CheckedFunction4<T1, T2, T3, T4, R> {
        R apply(T1 t1, T2 t2, T3 t3, T4 t4) throws Exception;
    }

    static public <R> R prepare1Instance(boolean setupDataByDefault,
            CheckedFunction2<JdbcDatabaseContainer<?>, Connection, R> lambda) throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse = startClickHouse();
                Connection connection = DriverManager.getConnection(clickhouse.getJdbcUrl())) {
            if (setupDataByDefault) {
                setupData(connection, (data) -> {
                    data.createTable();
                    data.insertData(1, 2L, "2", "2021-01-01");
                    data.insertData(2, 3L, "3", "2021-01-01");
                    data.insertData(3, 4L, "3", "2021-01-02");
                    data.insertData(4, 5L, "3", "2021-01-06");
                    data.insertData(5, 6L, "2", "2021-01-31");
                    data.insertData(6, 7L, "2", "2021-01-11");
                    data.insertData(7, 3L, "4", "2021-01-04");
                    return true;
                });
            }
            return lambda.apply(clickhouse, connection);
        }
    }

    static public <R> R prepare2Instances(boolean setupDataByDefault,
            CheckedFunction4<JdbcDatabaseContainer<?>, Connection, JdbcDatabaseContainer<?>, Connection, R> lambda)
            throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = startClickHouse();
                Connection connection1 = DriverManager.getConnection(clickhouse1.getJdbcUrl());
                JdbcDatabaseContainer<?> clickhouse2 = startClickHouse();
                Connection connection2 = DriverManager.getConnection(clickhouse2.getJdbcUrl())) {
            Assert.assertNotSame(clickhouse1.getJdbcUrl(), clickhouse2.getJdbcUrl());
            if (setupDataByDefault) {
                setupData(connection1, (data) -> {
                    data.createTable();
                    data.insertData(1, 2L, "2", "2021-01-01");
                    data.insertData(2, 3L, "3", "2021-01-01");
                    data.insertData(3, 4L, "3", "2021-01-02");
                    data.insertData(7, 3L, "4", "2021-01-04");
                    return true;
                });
                setupData(connection2, (data) -> {
                    data.createTable();
                    data.insertData(4, 5L, "3", "2021-01-06");
                    data.insertData(5, 6L, "2", "2021-01-31");
                    data.insertData(6, 7L, "2", "2021-01-11");
                    return true;
                });
            }
            return lambda.apply(clickhouse1, connection1, clickhouse2, connection2);
        }
    }

    static public class PrepareTestData {

        public static final String db = "default";
        public static final String table = "shard_table";
        private final ClickHouseRender render = new ClickHouseRender();
        private final Connection connection;

        public PrepareTestData(Connection connection) {
            this.connection = connection;
        }

        private void singleQuery(Connection connection, String sql) throws SQLException {
            try (Statement stmt = connection.createStatement()) {
                stmt.execute(sql);
            }
        }

        public void createTable() throws SQLException {
            final ClickHouseCreateTable create = ClickHouseCreateTable.createCKTableIgnoreExist(db, table)
                    .columns(new ColumnWithType("i1", "Int32")).columns(new ColumnWithType("i2", "Nullable(Int64)"))
                    .columns(new ColumnWithType("s2", "String")).columns(new ColumnWithType("n3", "Decimal(19,4)"))
                    .columns(new ColumnWithType("n4", "Decimal(19,0)"))
                    .columns(new ColumnWithType("d4", "Nullable(Date)"))
                    .columns(new ColumnWithType("str_date4", "Nullable(String)")).engine("MergeTree()");
            singleQuery(connection, create.toSql(render));
        }

        public void insertData(int i1, Long i2, String s2, String date0) throws SQLException {
            final InsertInto insertInto = InsertInto.insertInto(db, table).set("i1", i1).set("i2", i2).set("s2", s2)
                    .set("n3", -18.22).set("n4", -18).set("str_date4", date0);
            singleQuery(connection, insertInto.toSql(render));
        }
    }

    static public <R> R setupData(Connection connection, CheckedFunction<PrepareTestData, R> lambda) throws Exception {
        PrepareTestData prepareTestData = new PrepareTestData(connection);
        return lambda.apply(prepareTestData);
    }

    public static <T> T configClickhouseWith(JdbcDatabaseContainer<?>[] clickhouse, int replica, String queryCatalog,
            final Callable<T> lambda) throws Exception {
        internalConfigClickHouse(clickhouse, replica);
        Unsafe.setProperty(CONFIG_CLICKHOUSE_QUERY_CATALOG, queryCatalog);
        try {
            return lambda.call();
        } finally {
            Unsafe.clearProperty(CONFIG_CLICKHOUSE_QUERY_CATALOG);
        }
    }

    public static void internalConfigClickHouse(JdbcDatabaseContainer<?>[] clickhouse, int replica) throws IOException {
        Preconditions.checkArgument(clickhouse.length % replica == 0);
        Map<String, List<Node>> clusterNode = new HashMap<>();
        int pairNum = clickhouse.length / replica;
        IntStream.range(0, pairNum).forEach(idx -> clusterNode.put("pair" + idx, new ArrayList<>()));
        ClusterInfo cluster = new ClusterInfo().setKeepAliveTimeout("600000").setSocketTimeout("600000")
                .setConnectTimeout("3000").setExtConfig("maxWait=10").setCluster(clusterNode);
        int i = 0;
        for (JdbcDatabaseContainer<?> jdbcDatabaseContainer : clickhouse) {
            Node node = new Node();
            node.setName(String.format(Locale.ROOT, "node%02d", i));
            URI uri = URI.create(jdbcDatabaseContainer.getJdbcUrl().replace("jdbc:", ""));
            node.setIp(uri.getHost());
            node.setPort(uri.getPort());
            node.setUser("default");
            clusterNode.get("pair" + i % pairNum).add(node);
            i += 1;
        }
        File file = File.createTempFile("clickhouse", ".yaml");
        ClickHouseConfigLoader.getConfigYaml().dump(JsonUtil.readValue(JsonUtil.writeValueAsString(cluster), Map.class),
                new PrintWriter(file, Charset.defaultCharset().name()));
        Unsafe.setProperty(CONFIG_SECOND_STORAGE_CLUSTER, file.getAbsolutePath());
        Unsafe.setProperty(SecondStorageConstants.NODE_REPLICA, String.valueOf(replica));
        SecondStorage.init(true);
    }

    public static void internalConfigClickHouse(JdbcDatabaseContainer<?>[] clickhouse, int replica, int sshPort)
            throws IOException {
        Preconditions.checkArgument(clickhouse.length % replica == 0);
        Map<String, List<Node>> clusterNode = new HashMap<>();
        int pairNum = clickhouse.length / replica;
        IntStream.range(0, pairNum).forEach(idx -> clusterNode.put("pair" + idx, new ArrayList<>()));
        ClusterInfo cluster = new ClusterInfo().setKeepAliveTimeout("600000").setSocketTimeout("600000")
                .setConnectTimeout("3000").setExtConfig("maxWait=10").setCluster(clusterNode);
        int i = 0;
        for (JdbcDatabaseContainer<?> jdbcDatabaseContainer : clickhouse) {
            Node node = new Node();
            node.setName(String.format(Locale.ROOT, "node%02d", i));
            URI uri = URI.create(jdbcDatabaseContainer.getJdbcUrl().replace("jdbc:", ""));
            node.setIp(uri.getHost());
            node.setPort(uri.getPort());
            node.setUser("default");
            node.setSSHPort(sshPort);
            clusterNode.get("pair" + i % pairNum).add(node);
            i += 1;
        }
        File file = File.createTempFile("clickhouse", ".yaml");
        ClickHouseConfigLoader.getConfigYaml().dump(JsonUtil.readValue(JsonUtil.writeValueAsString(cluster), Map.class),
                new PrintWriter(file, Charset.defaultCharset().name()));
        Unsafe.setProperty(CONFIG_SECOND_STORAGE_CLUSTER, file.getAbsolutePath());
        Unsafe.setProperty(SecondStorageConstants.NODE_REPLICA, String.valueOf(replica));
        SecondStorage.init(true);
    }

    public static boolean findShardJDBCTable(LogicalPlan logicalPlan) {
        return !logicalPlan.find(new AbstractFunction1<LogicalPlan, Object>() {
            @Override
            public Object apply(LogicalPlan v1) {
                if (v1 instanceof DataSourceV2ScanRelation && (((DataSourceV2ScanRelation) v1).relation() != null)) {
                    DataSourceV2Relation relation = ((DataSourceV2ScanRelation) v1).relation();
                    return relation.table() instanceof ShardJDBCTable;
                }
                return false;
            }
        }).isEmpty();
    }

    static public Optional<DataSourceV2ScanRelation> findDataSourceV2ScanRelation(LogicalPlan logicalPlan) {
        return new RichOption<>(logicalPlan.find(new AbstractFunction1<LogicalPlan, Object>() {
            @Override
            public Object apply(LogicalPlan v1) {
                return v1 instanceof DataSourceV2ScanRelation;
            }
        })).toOptional().map(logical -> (DataSourceV2ScanRelation) logical);
    }

    static public JDBCScan findJDBCScan(LogicalPlan logicalPlan) {
        Optional<DataSourceV2ScanRelation> plan = findDataSourceV2ScanRelation(logicalPlan);

        Assert.assertTrue(plan.isPresent());
        Assert.assertTrue(plan.get().scan() instanceof V1ScanWrapper);
        V1ScanWrapper wrapper = (V1ScanWrapper) plan.get().scan();
        Assert.assertTrue(wrapper.v1Scan() instanceof JDBCScan);
        return (JDBCScan) wrapper.v1Scan();
    }

    public static void checkAggregateRemoved(Dataset ds) {
        checkAggregateRemoved(ds, true);
    }

    public static void checkAggregateRemoved(Dataset ds, boolean removed) {
        Optional<Aggregate> optional = new RichOption<>(
                ds.queryExecution().optimizedPlan().find(new AbstractFunction1<LogicalPlan, Object>() {
                    @Override
                    public Object apply(LogicalPlan v1) {
                        return v1 instanceof Aggregate;
                    }
                })).toOptional().map(logical -> (Aggregate) logical);
        if (removed) {
            assert !optional.isPresent();
        } else {
            assert optional.isPresent();
        }
    }

    public static void checkPushedInfo(Dataset ds, String... keywords) {
        Optional<DataSourceV2ScanRelation> v2ScanRelation = ClickHouseUtils
                .findDataSourceV2ScanRelation(ds.queryExecution().optimizedPlan());
        Assert.assertTrue(v2ScanRelation.isPresent());
        checkKeywordsExistsInExplain(ds, keywords);
    }

    private static void checkKeywordsExistsInExplain(Dataset ds, String... keywords) {
        checkKeywordsExistsInExplain(ds, ExplainMode.fromString("extended"), keywords);
    }

    private static void checkKeywordsExistsInExplain(Dataset ds, ExplainMode mode, String... keywords) {
        String normalizedOutput = getNormalizedExplain(ds, mode);
        for (String key : keywords) {
            assert normalizedOutput.contains(key);
        }
    }

    private static String getNormalizedExplain(Dataset ds, ExplainMode mode) {
        String output = ds.queryExecution().explainString(ExplainMode.fromString(mode.name()));
        return output.replaceAll("#\\d+", "#x");
    }

    static public FilePruner findFilePruner(LogicalPlan logicalPlan) {
        return new RichOption<>(logicalPlan.find(new AbstractFunction1<LogicalPlan, Object>() {
            @Override
            public Object apply(LogicalPlan v1) {
                if (v1 instanceof LogicalRelation && ((LogicalRelation) v1).relation() instanceof HadoopFsRelation) {
                    HadoopFsRelation fsRelation = (HadoopFsRelation) (((LogicalRelation) v1).relation());
                    return fsRelation.location() instanceof FilePruner;
                } else {
                    return false;
                }
            }
        })).toOptional().map(logical -> (HadoopFsRelation) (((LogicalRelation) logical).relation()))
                .map(fsRelation -> (FilePruner) fsRelation.location())
                .orElseThrow(() -> new IllegalStateException(" no FilePruner found"));
    }

    /* See
      1. src/examples/test_case_data/localmeta/metadata/table_index/table/DEFAULT.TEST_KYLIN_FACT.json
        2. src/examples/test_case_data/localmeta/metadata/table_index_incremental/table/DEFAULT.TEST_KYLIN_FACT.json
         * PRICE  <=>  9, hence its column name in ck is c9
    */
    public static final Map<String, String> columnMapping = ImmutableMap.of("PRICE", "c9");

    public static void InjectNewPushDownRule(SparkConf conf) {
        conf.set("spark.sql.extensions", "org.apache.kylin.query.SQLPushDownExtensions");
    }

    public static JobParam triggerClickHouseJob(NDataflow df) {
        val segments = new HashSet<>(df.getSegments());
        AbstractJobHandler localHandler = new SecondStorageSegmentLoadJobHandler();
        JobParam jobParam = SecondStorageJobParamUtil.of(df.getProject(), df.getModel().getUuid(), "ADMIN",
                segments.stream().map(NDataSegment::getId));
        String jobId = simulateJobMangerAddJob(jobParam, localHandler);
        waitJobFinish(df.getProject(), jobId);
        return jobParam;
    }

    public static String simulateJobMangerAddJob(JobParam jobParam, AbstractJobHandler handler) {
        ExecutableUtil.computeParams(jobParam);
        handler.handle(jobParam);
        return jobParam.getJobId();
    }

    public static void waitJobFinish(String project, String jobId, boolean isAllowFailed) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NExecutableManager executableManager = NExecutableManager.getInstance(config, project);
        DefaultExecutable job = (DefaultExecutable) executableManager.getJob(jobId);
        await().atMost(300, TimeUnit.SECONDS).until(() -> !job.getStatus().isProgressing());
        Assert.assertFalse(job.getStatus().isProgressing());
        if (!isAllowFailed) {
            val firstErrorMsg = IndexDataConstructor.firstFailedJobErrorMessage(executableManager, job);
            Assert.assertEquals(firstErrorMsg, ExecutableState.SUCCEED, executableManager.getJob(jobId).getStatus());
        }
    }

    private static void waitJobFinish(String project, String jobId) {
        waitJobFinish(project, jobId, false);
    }
}
