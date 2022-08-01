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
package org.apache.kylin.engine.spark.source;

import java.util.Arrays;

import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.junit.Assert;
import org.junit.Test;

public class NSparkTableReaderTest extends NLocalWithSparkSessionTest {

    @Test
    public void testReadData() throws Exception {

        populateSSWithCSVData(getTestConfig(), "default", ss);
        NSparkTableReader sparkTableReader = new NSparkTableReader("default", "test_kylin_fact");
        Assert.assertTrue(sparkTableReader.next());
        int i = 1;
        while (sparkTableReader.next()) {
            i++;
            String[] row = sparkTableReader.getRow();
            System.out.println(Arrays.toString(row));
            Assert.assertTrue(row != null);
        }
        int count = SparkSqlUtil.queryAll(ss, "test_kylin_fact").size();
        Assert.assertTrue(i == count);
        sparkTableReader.close();
    }
}
