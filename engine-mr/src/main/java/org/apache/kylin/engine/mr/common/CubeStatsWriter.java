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

package org.apache.kylin.engine.mr.common;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.measure.BufferedMeasureCodec;
import org.apache.kylin.measure.hllc.HLLCounter;

public class CubeStatsWriter {

    public static void writeCuboidStatistics(Configuration conf, Path outputPath, //
            Map<Long, HLLCounter> cuboidHLLMap, int samplingPercentage) throws IOException {
        writeCuboidStatistics(conf, outputPath, cuboidHLLMap, samplingPercentage, 0, 0);
    }

    public static void writeCuboidStatistics(Configuration conf, Path outputPath, //
            Map<Long, HLLCounter> cuboidHLLMap, int samplingPercentage, int mapperNumber, double mapperOverlapRatio) throws IOException {
        Path seqFilePath = new Path(outputPath, BatchConstants.CFG_STATISTICS_CUBOID_ESTIMATION_FILENAME);

        List<Long> allCuboids = new ArrayList<Long>();
        allCuboids.addAll(cuboidHLLMap.keySet());
        Collections.sort(allCuboids);

        ByteBuffer valueBuf = ByteBuffer.allocate(BufferedMeasureCodec.DEFAULT_BUFFER_SIZE);
        SequenceFile.Writer writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(seqFilePath), SequenceFile.Writer.keyClass(LongWritable.class), SequenceFile.Writer.valueClass(BytesWritable.class));
        try {
            // mapper overlap ratio at key -1
            writer.append(new LongWritable(-1), new BytesWritable(Bytes.toBytes(mapperOverlapRatio)));
            
            // mapper number at key -2
            writer.append(new LongWritable(-2), new BytesWritable(Bytes.toBytes(mapperNumber)));

            // sampling percentage at key 0
            writer.append(new LongWritable(0L), new BytesWritable(Bytes.toBytes(samplingPercentage)));

            for (long i : allCuboids) {
                valueBuf.clear();
                cuboidHLLMap.get(i).writeRegisters(valueBuf);
                valueBuf.flip();
                writer.append(new LongWritable(i), new BytesWritable(valueBuf.array(), valueBuf.limit()));
            }
        } finally {
            IOUtils.closeQuietly(writer);
        }
    }

}
