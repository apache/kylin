/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kylinolap.job.hadoop.cube;

import static org.junit.Assert.*;

import java.io.File;
import java.nio.ByteBuffer;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.kylinolap.common.util.LocalFileMetadataTestCase;
import com.kylinolap.cube.CubeManager;
import com.kylinolap.cube.kv.RowConstants;
import com.kylinolap.cube.measure.MeasureCodec;
import com.kylinolap.cube.model.CubeDesc;

/**
 * @author yangli9
 * 
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class CubeHFileMapper2Test extends LocalFileMetadataTestCase {

    String cubeName = "test_kylin_cube_with_slr_ready";

    MeasureCodec codec;
    ByteBuffer buf = ByteBuffer.allocate(RowConstants.ROWVALUE_BUFFER_SIZE);
    Object[] outKV = new Object[2];

    @Before
    public void setup() throws Exception {
        this.createTestMetadata();
        // hack for distributed cache
        FileUtils.deleteDirectory(new File("../job/meta"));
        FileUtils.copyDirectory(new File(this.getTestConfig().getMetadataUrl()), new File("../job/meta"));
        CubeDesc desc = CubeManager.getInstance(this.getTestConfig()).getCube(cubeName).getDescriptor();
        codec = new MeasureCodec(desc.getMeasures());
    }

    @After
    public void after() throws Exception {
        cleanupTestMetadata();
        FileUtils.deleteDirectory(new File("../job/meta"));
    }

    @Test
    public void testBasic() throws Exception {

        Configuration hconf = new Configuration();
        Context context = MockupMapContext.create(hconf, this.getTestConfig().getMetadataUrl(), cubeName, outKV);

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
