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
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.engine.mr.KylinMapper;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UHCDictionaryMapper extends KylinMapper<NullWritable, Text, SelfDefineSortableKey, NullWritable> {
    private static final Logger logger = LoggerFactory.getLogger(UHCDictionaryMapper.class);

    protected int index;
    protected DataType type;

    protected Text outputKey = new Text();
    private ByteBuffer tmpBuf;
    private SelfDefineSortableKey sortableKey = new SelfDefineSortableKey();

    @Override
    protected void doSetup(Context context) throws IOException {
        tmpBuf = ByteBuffer.allocate(4096);

        Configuration conf = context.getConfiguration();
        bindCurrentConfiguration(conf);
        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();

        CubeInstance cube = CubeManager.getInstance(config).getCube(conf.get(BatchConstants.CFG_CUBE_NAME));
        List<TblColRef> uhcColumns = cube.getDescriptor().getAllUHCColumns();

        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String colName = fileSplit.getPath().getParent().getName();

        for (int i = 0; i < uhcColumns.size(); i++) {
            if (uhcColumns.get(i).getIdentity().equalsIgnoreCase(colName)) {
                index = i;
                break;
            }
        }
        type = uhcColumns.get(index).getType();

        //for debug
        logger.info("column name: " + colName);
        logger.info("index: " + index);
        logger.info("type: " + type);
    }

    @Override
    public void doMap(NullWritable key, Text value, Context context) throws IOException, InterruptedException {
        tmpBuf.clear();
        int size = value.getLength()+ 1;
        if (size >= tmpBuf.capacity()) {
            tmpBuf = ByteBuffer.allocate(countNewSize(tmpBuf.capacity(), size));
        }
        tmpBuf.put(Bytes.toBytes(index)[3]);
        tmpBuf.put(value.getBytes(), 0, value.getLength());
        outputKey.set(tmpBuf.array(), 0, tmpBuf.position());

        sortableKey.init(outputKey, type);
        context.write(sortableKey, NullWritable.get());
    }

    private int countNewSize(int oldSize, int dataSize) {
        int newSize = oldSize * 2;
        while (newSize < dataSize) {
            newSize = newSize * 2;
        }
        return newSize;
    }
}
