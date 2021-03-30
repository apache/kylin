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

package org.apache.kylin.job.streaming;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Random;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kylin.shaded.com.google.common.base.Preconditions;
import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.apache.kylin.shaded.com.google.common.collect.SortedMultiset;
import org.apache.kylin.shaded.com.google.common.collect.TreeMultiset;

/**
 * this is for generating fact table data for test_streaming_table (cube streaming)
 */
public class StreamingTableDataGenerator {

    private static final Logger logger = LoggerFactory.getLogger(StreamingTableDataGenerator.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final String COLUMN_TIMESTAMP = "timestamp";
    private static final String COLUMN_ITEM = "itm";
    private static final String COLUMN_CATEGORY = "category_id";

    public static List<String> generate(int recordCount, long startTime, long endTime, String tableName, String prj) {
        Preconditions.checkArgument(startTime < endTime);
        Preconditions.checkArgument(recordCount > 0);

        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        TableDesc tableDesc = TableMetadataManager.getInstance(kylinConfig).getTableDesc(tableName, prj);

        SortedMultiset<Long> times = TreeMultiset.create();
        Random r = new Random();
        for (int i = 0; i < recordCount; i++) {
            long t = startTime + (long) ((endTime - startTime) * r.nextDouble());
            times.add(t);
        }

        List<String> ret = Lists.newArrayList();
        HashMap<String, String> kvs = Maps.newHashMap();
        HashMap<String, Integer> itemCategory = Maps.newHashMap();

        for (long time : times) {
            kvs.clear();
            kvs.put(COLUMN_TIMESTAMP, String.valueOf(time));
            for (ColumnDesc columnDesc : tableDesc.getColumns()) {
                String lowerCaseColumnName = columnDesc.getName().toLowerCase(Locale.ROOT);
                DataType dataType = columnDesc.getType();
                if (dataType.isDateTimeFamily()) {
                    //TimedJsonStreamParser will derived minute_start,hour_start,day_start from timestamp
                    continue;
                } else if (dataType.isStringFamily()) {
                    char c = (char) ('A' + (int) (26 * r.nextDouble()));
                    kvs.put(lowerCaseColumnName, String.valueOf(c));
                } else if (dataType.isIntegerFamily()) {
                    int v;
                    // generate category_id for joined lookup table
                    if (COLUMN_CATEGORY.equals(lowerCaseColumnName)) {
                        String itm = kvs.get(COLUMN_ITEM);
                        if (itemCategory.get(itm) == null) {
                            v = r.nextInt(10);
                            itemCategory.put(itm, v);
                        } else {
                            v = itemCategory.get(itm);
                        }
                    } else {
                        v = r.nextInt(10000);
                    }
                    kvs.put(lowerCaseColumnName, String.valueOf(v));
                } else if (dataType.isNumberFamily()) {
                    String v = String.format(Locale.ROOT, "%.4f", r.nextDouble() * 100);
                    kvs.put(lowerCaseColumnName, v);
                }
            }
            try {
                ret.add(mapper.writeValueAsString(kvs));
            } catch (JsonProcessingException e) {
                logger.error("error!", e);
            }
        }

        return ret;
    }
}
