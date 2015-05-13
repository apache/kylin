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

package org.apache.kylin.job;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.MapType;
import com.fasterxml.jackson.databind.type.SimpleType;
import com.google.common.base.Function;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.common.util.SortUtil;
import org.apache.kylin.job.dataGen.FactTableGenerator;
import org.apache.kylin.job.dataGen.StreamingDataGenerator;
import org.apache.kylin.metadata.MetadataManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.assertTrue;

/**
 */
public class DataGenTest extends LocalFileMetadataTestCase {

    @Before
    public void before() throws Exception {
        this.createTestMetadata();
        MetadataManager.clearCache();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testBasics() throws Exception {
        String content = FactTableGenerator.generate("test_kylin_cube_with_slr_ready", "10000", "1", null, "inner");// default  settings
        System.out.println(content);
        assertTrue(content.contains("FP-non GTC"));
        assertTrue(content.contains("ABIN"));

        DeployUtil.overrideFactTableData(content, "default.test_kylin_fact");
    }

    @Test
    public void testStreaming() throws Exception {
        int totalCount = 10000;
        int counter = 0;

        Iterator<String> iterator = StreamingDataGenerator.generate(DateFormat.stringToMillis("2015-01-03"), DateFormat.stringToMillis("2015-02-05"), totalCount);

        iterator = SortUtil.extractAndSort(iterator, new Function<String, Comparable>() {
            public Comparable apply(String input) {
                return getTsStr(input);
            }
        });

        //FileUtils.writeLines(new File("//Users/honma/streaming_table_data"),Lists.newArrayList(iterator));

        long lastTs = 0;
        while (iterator.hasNext()) {
            counter++;
            String row = iterator.next();
            System.out.println(row);
            long ts = Long.parseLong(getTsStr(row));
            Assert.assertTrue(ts >= lastTs);
            lastTs = ts;
        }
        Assert.assertEquals(totalCount, counter);
    }

    final JavaType javaType = MapType.construct(HashMap.class, SimpleType.construct(String.class), SimpleType.construct(String.class));
    final ObjectMapper objectMapper = new ObjectMapper();

    private String getTsStr(String input) {
        Map<String, String> json ;
        try {
            json = objectMapper.readValue(input.getBytes(), javaType);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return json.get("ts");
    }
}
