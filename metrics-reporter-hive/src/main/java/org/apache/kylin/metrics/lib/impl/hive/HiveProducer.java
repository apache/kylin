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

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metrics.lib.ActiveReservoirReporter;
import org.apache.kylin.metrics.lib.Record;
import org.apache.kylin.metrics.lib.impl.TimePropertyEnum;
import org.apache.kylin.metrics.lib.impl.hive.HiveProducerRecord.RecordKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class HiveProducer {

    private static final Logger logger = LoggerFactory.getLogger(HiveProducer.class);

    private static final int CACHE_MAX_SIZE = 10;

    private final HiveConf hiveConf;
    private final FileSystem hdfs;
    private final LoadingCache<Pair<String, String>, Pair<String, List<FieldSchema>>> tableFieldSchemaCache;
    private final String CONTENT_FILE_NAME;

    public HiveProducer(Properties props) throws Exception {
        this(props, new HiveConf());
    }

    HiveProducer(Properties props, HiveConf hiveConfig) throws Exception {
        hiveConf = hiveConfig;
        for (Map.Entry<Object, Object> e : props.entrySet()) {
            hiveConf.set(e.getKey().toString(), e.getValue().toString());
        }

        hdfs = FileSystem.get(hiveConf);

        tableFieldSchemaCache = CacheBuilder.newBuilder().removalListener(new RemovalListener<Pair<String, String>, Pair<String, List<FieldSchema>>>() {
            @Override
            public void onRemoval(RemovalNotification<Pair<String, String>, Pair<String, List<FieldSchema>>> notification) {
                logger.info("Field schema with table " + ActiveReservoirReporter.getTableName(notification.getKey()) + " is removed due to " + notification.getCause());
            }
        }).maximumSize(CACHE_MAX_SIZE).build(new CacheLoader<Pair<String, String>, Pair<String, List<FieldSchema>>>() {
            @Override
            public Pair<String, List<FieldSchema>> load(Pair<String, String> tableName) throws Exception {
                HiveMetaStoreClient metaStoreClient = new HiveMetaStoreClient(hiveConf);
                String tableLocation = metaStoreClient.getTable(tableName.getFirst(), tableName.getSecond()).getSd().getLocation();
                List<FieldSchema> fields = metaStoreClient.getFields(tableName.getFirst(), tableName.getSecond());
                metaStoreClient.close();
                return new Pair(tableLocation, fields);
            }
        });

        String hostName;
        try {
            hostName = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            hostName = "UNKNOWN";
        }
        CONTENT_FILE_NAME = hostName + "-part-0000";
    }

    public void close() {
        tableFieldSchemaCache.cleanUp();
    }

    public void send(final Record record) throws Exception {
        HiveProducerRecord hiveRecord = convertTo(record);
        write(hiveRecord.key(), Lists.newArrayList(hiveRecord));
    }

    public void send(final List<Record> recordList) throws Exception {
        Map<RecordKey, List<HiveProducerRecord>> recordMap = Maps.newHashMap();
        for (Record record : recordList) {
            HiveProducerRecord hiveRecord = convertTo(record);
            if (recordMap.get(hiveRecord.key()) == null) {
                recordMap.put(hiveRecord.key(), Lists.<HiveProducerRecord> newLinkedList());
            }
            recordMap.get(hiveRecord.key()).add(hiveRecord);
        }

        for (Map.Entry<RecordKey, List<HiveProducerRecord>> entry : recordMap.entrySet()) {
            write(entry.getKey(), entry.getValue());
        }
    }

    private void write(RecordKey recordKey, Iterable<HiveProducerRecord> recordItr) throws Exception {
        String tableLocation = tableFieldSchemaCache.get(new Pair<>(recordKey.database(), recordKey.table())).getFirst();
        StringBuilder sb = new StringBuilder();
        sb.append(tableLocation);
        for (Map.Entry<String, String> e : recordKey.partition().entrySet()) {
            sb.append("/");
            sb.append(e.getKey().toLowerCase(Locale.ROOT));
            sb.append("=");
            sb.append(e.getValue());
        }
        Path partitionPath = new Path(sb.toString());
        if (!hdfs.exists(partitionPath)) {
            StringBuilder hql = new StringBuilder();
            hql.append("ALTER TABLE ");
            hql.append(recordKey.database() + "." + recordKey.table());
            hql.append(" ADD IF NOT EXISTS PARTITION (");
            boolean ifFirst = true;
            for (Map.Entry<String, String> e : recordKey.partition().entrySet()) {
                if (ifFirst) {
                    ifFirst = false;
                } else {
                    hql.append(",");
                }
                hql.append(e.getKey().toLowerCase(Locale.ROOT));
                hql.append("='" + e.getValue() + "'");
            }
            hql.append(")");
            Driver driver = new Driver(hiveConf);
            SessionState.start(new CliSessionState(hiveConf));
            driver.run(hql.toString());
            driver.close();
        }
        Path partitionContentPath = new Path(partitionPath, CONTENT_FILE_NAME);
        if (!hdfs.exists(partitionContentPath)) {
            int nRetry = 0;
            while (!hdfs.createNewFile(partitionContentPath) && nRetry++ < 5) {
                if (hdfs.exists(partitionContentPath)) {
                    break;
                }
                Thread.sleep(500L * nRetry);
            }
            if (!hdfs.exists(partitionContentPath)) {
                throw new RuntimeException("Fail to create HDFS file: " + partitionContentPath + " after " + nRetry + " retries");
            }
        }
        try (FSDataOutputStream fout = hdfs.append(partitionContentPath)) {
            for (HiveProducerRecord elem : recordItr) {
                fout.writeBytes(elem.valueToString() + "\n");
            }
        } catch (IOException e) {
            System.out.println("Fails to write metrics to file " + partitionContentPath.toString() + " due to " + e);
            logger.error("Fails to write metrics to file " + partitionContentPath.toString() + " due to " + e);
        }
    }

    private HiveProducerRecord convertTo(Record record) throws Exception {
        Map<String, Object> rawValue = record.getValueRaw();

        //Set partition values for hive table
        Map<String, String> partitionKVs = Maps.newHashMapWithExpectedSize(1);
        partitionKVs.put(TimePropertyEnum.DAY_DATE.toString(), rawValue.get(TimePropertyEnum.DAY_DATE.toString()).toString());

        return parseToHiveProducerRecord(HiveReservoirReporter.getTableFromSubject(record.getType()), partitionKVs, rawValue);
    }

    public HiveProducerRecord parseToHiveProducerRecord(String tableName, Map<String, String> partitionKVs, Map<String, Object> rawValue) throws Exception {
        Pair<String, String> tableNameSplits = ActiveReservoirReporter.getTableNameSplits(tableName);
        List<FieldSchema> fields = tableFieldSchemaCache.get(tableNameSplits).getSecond();
        List<Object> columnValues = Lists.newArrayListWithExpectedSize(fields.size());
        for (FieldSchema fieldSchema : fields) {
            columnValues.add(rawValue.get(fieldSchema.getName().toUpperCase(Locale.ROOT)));
        }

        return new HiveProducerRecord(tableNameSplits.getFirst(), tableNameSplits.getSecond(), partitionKVs, columnValues);
    }

}
