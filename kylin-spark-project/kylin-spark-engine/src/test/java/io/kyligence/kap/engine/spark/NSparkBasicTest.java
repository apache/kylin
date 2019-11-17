/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.kyligence.kap.engine.spark;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Lists;

@Ignore("convenient trial tool for dev")
@SuppressWarnings("unused")
public class NSparkBasicTest extends NLocalWithSparkSessionTest {
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
