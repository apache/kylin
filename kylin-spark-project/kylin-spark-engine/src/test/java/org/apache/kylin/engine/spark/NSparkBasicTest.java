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

package org.apache.kylin.engine.spark;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

@Ignore("convenient trial tool for dev")
@SuppressWarnings("unused")
public class NSparkBasicTest extends LocalWithSparkSessionTest {
    @Test
    public void testToRdd() throws IOException {
        final String dataJson = "0,1,2,1000\n0,1,2,1\n3,4,5,2";
        File dataFile = File.createTempFile("tmp", ".csv");
        dataFile.deleteOnExit();
        FileUtils.writeStringToFile(dataFile, dataJson, Charset.defaultCharset());

        Dataset<Row> ds = ss.read().option("header", "true").csv(dataFile.getAbsolutePath()).repartition(2);
        Function<Row, Row> func = new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                return row;
            }
        };
        List<String> newNames = Lists.newArrayList();
        for (String name : ds.schema().fieldNames()) {
            newNames.add("new" + name);
        }
        ds = ds.toDF(newNames.toArray(new String[0])).map(new MapFunction<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                return row;
            }
        }, RowEncoder.apply(ds.schema()));
        System.out.println(ds.queryExecution().optimizedPlan());

    }
}
