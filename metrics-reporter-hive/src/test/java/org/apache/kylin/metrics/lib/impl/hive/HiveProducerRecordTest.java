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

import static org.apache.kylin.metrics.lib.impl.hive.HiveProducerRecord.DELIMITER;
import static org.apache.kylin.metrics.lib.impl.hive.HiveProducerRecord.RecordKey.DEFAULT_DB_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.metrics.lib.impl.hive.HiveProducerRecord.RecordKey;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class HiveProducerRecordTest {

    @Test
    public void testRecord() {
        String dbName = "KYLIN";
        String tableName = "test";
        Map<String, String> partitionKVs = Maps.newHashMap();
        partitionKVs.put("key1", "value1");

        Set<RecordKey> keySet = Sets.newHashSet();
        RecordKey key1 = new HiveProducerRecord.KeyBuilder(tableName).build();
        RecordKey key11 = new HiveProducerRecord.KeyBuilder(tableName).setDbName(DEFAULT_DB_NAME).build();
        keySet.add(key1);
        keySet.add(key11);
        assertEquals(1, keySet.size());

        RecordKey key2 = new HiveProducerRecord.KeyBuilder(tableName).setDbName(dbName).build();
        RecordKey key3 = new HiveProducerRecord.KeyBuilder(tableName).setDbName(dbName).setPartitionKVs(partitionKVs)
                .build();
        keySet.add(key2);
        keySet.add(key3);
        assertEquals(3, keySet.size());
        assertEquals(dbName, key2.database());
        assertEquals(tableName, key2.table());

        List<Object> value1 = Lists.<Object> newArrayList(1);
        List<Object> value2 = Lists.<Object> newArrayList(1, "1");

        assertNull(new HiveProducerRecord(key1, null).valueToString());

        Set<HiveProducerRecord> recordSet = Sets.newHashSet();
        HiveProducerRecord record1 = new HiveProducerRecord(key1, value1);
        HiveProducerRecord record11 = new HiveProducerRecord(key11, value1);
        recordSet.add(record1);
        recordSet.add(record11);
        assertEquals(1, recordSet.size());
        assertEquals(key1, record1.key());
        assertEquals(value1, record1.value());

        recordSet.add(new HiveProducerRecord(key1, value2));
        recordSet.add(new HiveProducerRecord(key2, value1));
        assertEquals(3, recordSet.size());
        assertEquals(1, record1.valueToString().split(DELIMITER).length);
    }
}
