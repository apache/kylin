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

package org.apache.kylin.metrics.lib.impl.hive;

import static org.junit.Assert.assertEquals;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metrics.lib.ActiveReservoirReporter;
import org.apache.kylin.metrics.lib.impl.RecordEvent;
import org.apache.kylin.metrics.lib.impl.TimePropertyEnum;
import org.apache.kylin.metrics.lib.impl.TimedRecordEvent;
import org.apache.kylin.metrics.property.QueryRPCPropertyEnum;
import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.apache.kylin.source.hive.HiveMetaStoreClientFactory;
import org.apache.kylin.tool.metrics.systemcube.HiveTableCreator;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.rule.PowerMockRule;

@PrepareForTest(fullyQualifiedNames = { "org.apache.hadoop.fs.FileSystem",
        "org.apache.kylin.source.hive.HiveMetaStoreClientFactory",
        "org.apache.kylin.metrics.lib.impl.hive.HiveProducer$1" })
public class HiveProducerTest {

    @Rule
    public PowerMockRule rule = new PowerMockRule();

    private HiveProducer hiveProducer;
    private HiveMetaStoreClient metaStoreClient;

    @Before
    public void setUp() throws Exception {
        System.setProperty(KylinConfig.KYLIN_CONF, "../examples/test_case_data/localmeta");

        FileSystem hdfs = PowerMockito.mock(FileSystem.class);
        URI uri = PowerMockito.mock(URI.class);
        PowerMockito.stub(PowerMockito.method(FileSystem.class, "get", Configuration.class)).toReturn(hdfs);
        PowerMockito.when(hdfs.getUri()).thenReturn(uri);
        PowerMockito.when(uri.toString()).thenReturn("hdfs");

        HiveConf hiveConf = PowerMockito.mock(HiveConf.class);
        String metricsType = new HiveSink()
                .getTableFromSubject(KylinConfig.getInstanceFromEnv().getKylinMetricsSubjectQueryRpcCall());

        hiveProducer = new HiveProducer(metricsType, new Properties(), hiveConf);

        metaStoreClient = PowerMockito.mock(HiveMetaStoreClient.class);
        PowerMockito.whenNew(HiveMetaStoreClient.class).withArguments(hiveConf).thenReturn(metaStoreClient);
        PowerMockito
                .stub(PowerMockito.method(HiveMetaStoreClientFactory.class, "getHiveMetaStoreClient", HiveConf.class))
                .toReturn(metaStoreClient);
    }

    @After
    public void after() throws Exception {
        System.clearProperty(KylinConfig.KYLIN_CONF);
    }

    @Test
    public void testProduce() throws Exception {
        TimedRecordEvent rpcEvent = generateTestRPCRecord();

        Map<String, String> partitionKVs = Maps.newHashMap();
        partitionKVs.put(TimePropertyEnum.DAY_DATE.toString(), rpcEvent.getDayDate());

        List<Object> value = Lists.newArrayList(rpcEvent.getHost(), "default", "test_cube", "sandbox", "NULL", 80L, 3L,
                3L, 0L, 0L, 0L, rpcEvent.getTime(), rpcEvent.getYear(), rpcEvent.getMonth(),
                rpcEvent.getWeekBeginDate(), rpcEvent.getDayTime(), rpcEvent.getTimeHour(), rpcEvent.getTimeMinute(),
                rpcEvent.getTimeSecond(), rpcEvent.getDayDate());

        HiveProducerRecord.RecordKey key = new HiveProducerRecord.KeyBuilder("HIVE_metrics_query_rpc_test")
                .setDbName("KYLIN").setPartitionKVs(partitionKVs).build();
        HiveProducerRecord target = new HiveProducerRecord(key, value);

        prepareMockForEvent(rpcEvent);
        assertEquals(target, hiveProducer.convertTo(rpcEvent));
    }

    private void prepareMockForEvent(RecordEvent event) throws Exception {
        String tableFullName = new HiveSink().getTableFromSubject(event.getEventType());
        Pair<String, String> tableNameSplits = ActiveReservoirReporter.getTableNameSplits(tableFullName);
        String dbName = tableNameSplits.getFirst();
        String tableName = tableNameSplits.getSecond();

        Table table = PowerMockito.mock(Table.class);
        PowerMockito.when(metaStoreClient, "getTable", dbName, tableName).thenReturn(table);

        StorageDescriptor sd = PowerMockito.mock(StorageDescriptor.class);
        PowerMockito.when(table, "getSd").thenReturn(sd);
        PowerMockito.when(sd, "getLocation").thenReturn(null);

        List<Pair<String, String>> columns = HiveTableCreator.getHiveColumnsForMetricsQueryRPC();
        List<Pair<String, String>> partitions = HiveTableCreator.getPartitionKVsForHiveTable();
        columns.addAll(partitions);
        List<FieldSchema> fields = Lists.newArrayListWithExpectedSize(columns.size());
        for (Pair<String, String> column : columns) {
            fields.add(new FieldSchema(column.getFirst(), column.getSecond(), null));
        }
        PowerMockito.when(metaStoreClient, "getFields", dbName, tableName).thenReturn(fields);
    }

    private TimedRecordEvent generateTestRPCRecord() {
        TimedRecordEvent rpcMetricsEvent = new TimedRecordEvent("metrics_query_rpc_test");
        setRPCWrapper(rpcMetricsEvent, "default", "test_cube", "sandbox", null);
        setRPCStats(rpcMetricsEvent, 80L, 0L, 3L, 3L, 0L);
        return rpcMetricsEvent;
    }

    private static void setRPCWrapper(RecordEvent metricsEvent, String projectName, String realizationName,
            String rpcServer, Throwable throwable) {
        metricsEvent.put(QueryRPCPropertyEnum.PROJECT.toString(), projectName);
        metricsEvent.put(QueryRPCPropertyEnum.REALIZATION.toString(), realizationName);
        metricsEvent.put(QueryRPCPropertyEnum.RPC_SERVER.toString(), rpcServer);
        metricsEvent.put(QueryRPCPropertyEnum.EXCEPTION.toString(),
                throwable == null ? "NULL" : throwable.getClass().getName());
    }

    private static void setRPCStats(RecordEvent metricsEvent, long callTimeMs, long skipCount, long scanCount,
            long returnCount, long aggrCount) {
        metricsEvent.put(QueryRPCPropertyEnum.CALL_TIME.toString(), callTimeMs);
        metricsEvent.put(QueryRPCPropertyEnum.SKIP_COUNT.toString(), skipCount); //Number of skips on region servers based on region meta or fuzzy filter
        metricsEvent.put(QueryRPCPropertyEnum.SCAN_COUNT.toString(), scanCount); //Count scanned by region server
        metricsEvent.put(QueryRPCPropertyEnum.RETURN_COUNT.toString(), returnCount);//Count returned by region server
        metricsEvent.put(QueryRPCPropertyEnum.AGGR_FILTER_COUNT.toString(), scanCount - returnCount); //Count filtered & aggregated by coprocessor
        metricsEvent.put(QueryRPCPropertyEnum.AGGR_COUNT.toString(), aggrCount); //Count aggregated by coprocessor
    }
}
