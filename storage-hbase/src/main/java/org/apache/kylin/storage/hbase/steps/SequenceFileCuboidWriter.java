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

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.mr.ByteArrayWritable;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.apache.kylin.engine.mr.JobBuilderSupport;
import org.apache.kylin.engine.mr.steps.KVGTRecordWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class SequenceFileCuboidWriter extends KVGTRecordWriter {

    private static final Logger logger = LoggerFactory.getLogger(SequenceFileCuboidWriter.class);
    private SequenceFile.Writer writer = null;

    public SequenceFileCuboidWriter(CubeDesc cubeDesc, CubeSegment segment) {
        super(cubeDesc, segment);
        try {
            initiate();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected void initiate() throws IOException {
        if (writer == null) {
            JobBuilderSupport jobBuilderSupport = new JobBuilderSupport(cubeSegment, "SYSTEM");
            String cuboidRoot = jobBuilderSupport.getCuboidRootPath(cubeSegment);
            Path cuboidPath = new Path(cuboidRoot);
            FileSystem fs = HadoopUtil.getFileSystem(cuboidRoot);
            try {
                if (fs.exists(cuboidPath)) {
                    fs.delete(cuboidPath, true);
                }

                fs.mkdirs(cuboidPath);
            } finally {
                IOUtils.closeQuietly(fs);
            }

            Path cuboidFile = new Path(cuboidPath, "data.seq");
            logger.debug("Cuboid is written to " + cuboidFile);
            writer = SequenceFile.createWriter(HadoopUtil.getCurrentConfiguration(), SequenceFile.Writer.file(cuboidFile), SequenceFile.Writer.keyClass(Text.class), SequenceFile.Writer.valueClass(Text.class));
        }

    }

    @Override
    protected void writeAsKeyValue(ByteArrayWritable key, ByteArrayWritable value) throws IOException {

        Text outputValue = new Text();
        Text outputKey = new Text();
        outputKey.set(key.array(), key.offset(), key.length());
        outputValue.set(value.array(), value.offset(), value.length());
        writer.append(outputKey, outputValue);
    }

    @Override
    public void flush() throws IOException {
        if (writer != null) {
            writer.hflush();
        }
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeQuietly(writer);
    }
}
