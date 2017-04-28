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

package org.apache.kylin.storage.hbase.steps;

import static org.junit.Assert.assertTrue;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.measure.MeasureCodec;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author yangli9
 * 
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class CubeHFileMapper2Test extends LocalFileMetadataTestCase {

    String cubeName = "test_kylin_cube_with_slr_ready";

    MeasureCodec codec;
    Object[] outKV = new Object[2];

    @Before
    public void setup() throws Exception {
        this.createTestMetadata();
        // hack for distributed cache
        FileUtils.deleteDirectory(new File("../job/meta"));
        FileUtils.copyDirectory(new File(getTestConfig().getMetadataUrl()), new File("../job/meta"));
        CubeDesc desc = CubeManager.getInstance(getTestConfig()).getCube(cubeName).getDescriptor();
        codec = new MeasureCodec(desc.getMeasures());
    }

    @After
    public void after() throws Exception {
        cleanupTestMetadata();
        FileUtils.deleteDirectory(new File("../job/meta"));
    }

    @Test
    public void testBasic() throws Exception {

        Configuration hconf = HadoopUtil.getCurrentConfiguration();
        Context context = MockupMapContext.create(hconf, getTestConfig().getMetadataUrl(), cubeName, outKV);

        CubeHFileMapper mapper = new CubeHFileMapper();
        mapper.setup(context);

        Text key = new Text("not important");
        Text value = new Text(new byte[] { 2, 2, 51, -79, 1 });

        mapper.map(key, value, context);

        ImmutableBytesWritable outKey = (ImmutableBytesWritable) outKV[0];
        KeyValue outValue = (KeyValue) outKV[1];

        assertTrue(Bytes.compareTo(key.getBytes(), 0, key.getLength(), outKey.get(), outKey.getOffset(), outKey.getLength()) == 0);

        assertTrue(Bytes.compareTo(value.getBytes(), 0, value.getLength(), outValue.getValueArray(), outValue.getValueOffset(), outValue.getValueLength()) == 0);
    }

}
