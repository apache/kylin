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

package org.apache.kylin.engine.mr.steps;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.kylin.common.util.HadoopUtil;
import org.junit.Ignore;
import org.junit.Test;

/**
 * @author yangli9
 * 
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class HiveToBaseCuboidMapperPerformanceTest {

    String metadataUrl = "hbase:yadesk00:2181:/hbase-unsecure";
    String cubeName = "test_kylin_cube_with_slr";
    Path srcPath = new Path("/download/test_kylin_cube_with_slr_intermediate_table_64mb.seq");

    @Ignore("convenient trial tool for dev")
    @Test
    public void test() throws IOException, InterruptedException {
        Configuration hconf = HadoopUtil.getCurrentConfiguration();
        HiveToBaseCuboidMapper mapper = new HiveToBaseCuboidMapper();
        Context context = MockupMapContext.create(hconf, metadataUrl, cubeName, null);

        mapper.doSetup(context);

        Reader reader = new Reader(hconf, SequenceFile.Reader.file(srcPath));
        Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), hconf);
        Text value = new Text();

        while (reader.next(key, value)) {
            mapper.map(key, value, context);
        }

        reader.close();
    }

}
