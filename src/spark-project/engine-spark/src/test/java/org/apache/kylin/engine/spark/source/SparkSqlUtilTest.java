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

import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.junit.Assert;
import org.junit.Test;

import lombok.val;


public class SparkSqlUtilTest extends NLocalWithSparkSessionTest {

    @Test
    public void getViewOrignalTables() throws Exception {
        val tableName = "tableX";
        try {
            //get table tableX
            ss.sql("CREATE TABLE IF NOT EXISTS tableX (a int, b int) using csv");

            //get view v_tableX
            ss.sql("CREATE VIEW IF NOT EXISTS v_tableX as select * from " + tableName);

            //get nested view v_tableX_nested
            ss.sql("CREATE VIEW IF NOT EXISTS v_tableX_nested as select * from v_tableX");

            Assert.assertEquals(tableName, SparkSqlUtil.getViewOrignalTables("v_tableX", ss).iterator().next());

            Assert.assertEquals(tableName, SparkSqlUtil.getViewOrignalTables("v_tableX_nested", ss).iterator().next());
        } finally {
            ss.sql("DROP VIEW IF EXISTS v_tableX_nested");
            ss.sql("DROP VIEW IF EXISTS v_tableX");
            ss.sql("DROP TABLE IF EXISTS " + tableName);
        }
    }
}