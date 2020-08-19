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

package org.apache.kylin.engine.mr.streaming;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.kv.AbstractRowKeyEncoder;
import org.apache.kylin.measure.BufferedMeasureCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ColumnarSplitDataReader extends ColumnarSplitReader {
    private static final Logger logger = LoggerFactory.getLogger(ColumnarSplitDataReader.class);

    private Cuboid baseCuboid;
    private AbstractRowKeyEncoder rowKeyEncoder;
    private ByteBuffer metricsValuesBuffer;

    private Text currentKey;
    private Text currentValue;
    private AtomicInteger rowCount;
    private RowRecordReader rowRecordReader;

    public ColumnarSplitDataReader(InputSplit inputSplit, TaskAttemptContext context) throws IOException,
            InterruptedException {
        super(inputSplit, context);
        init(inputSplit, context);
    }

    public void init(InputSplit split, TaskAttemptContext context) throws IOException {
        baseCuboid = Cuboid.getBaseCuboid(cubeDesc);
        rowKeyEncoder = AbstractRowKeyEncoder.createInstance(cubeSegment, baseCuboid);

        FileSystem fs = FileSystem.get(context.getConfiguration());
        FileSplit fSplit = (FileSplit) split;
        Path path = fSplit.getPath();
        rowRecordReader = new RowRecordReader(cubeDesc, path, fs);
        metricsValuesBuffer = ByteBuffer.allocate(BufferedMeasureCodec.DEFAULT_BUFFER_SIZE);

        rowCount = new AtomicInteger(0);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (rowRecordReader.hasNextRow()) {
            rowCount.getAndIncrement();
            RowRecord rowRecord = rowRecordReader.nextRow();
            String[] dimensionsValues = rowRecord.getDimensions();
            byte[] key = rowKeyEncoder.encode(dimensionsValues);
            currentKey = new Text();
            currentKey.set(key, 0, key.length);

            byte[][] metricsValues = rowRecord.getMetrics();
            currentValue = new Text();
            metricsValuesBuffer.clear();

            for (int i = 0; i < metricsValues.length; i++) {
                metricsValuesBuffer.put(metricsValues[i], 0, metricsValues[i].length);
            }
            currentValue.set(metricsValuesBuffer.array(), 0, metricsValuesBuffer.position());
            return true;
        }
        return false;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return currentValue;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return -1;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return currentKey;
    }

    @Override
    public void close() throws IOException {
        rowRecordReader.close();
    }

}
