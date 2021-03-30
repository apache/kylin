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

import org.apache.kylin.shaded.com.google.common.collect.Lists;

public class CubeStatsWriter {

    public static void writeCuboidStatistics(Configuration conf, Path outputPath, //
            Map<Long, HLLCounter> cuboidHLLMap, int samplingPercentage) throws IOException {
        writeCuboidStatistics(conf, outputPath, cuboidHLLMap, samplingPercentage, 0, 0, 0);
    }

    public static void writeCuboidStatistics(Configuration conf, Path outputPath, //
            Map<Long, HLLCounter> cuboidHLLMap, int samplingPercentage, long sourceRecordCoun) throws IOException {
        writeCuboidStatistics(conf, outputPath, cuboidHLLMap, samplingPercentage, 0, 0, sourceRecordCoun);
    }

    public static void writeCuboidStatistics(Configuration conf, Path outputPath, //
            Map<Long, HLLCounter> cuboidHLLMap, int samplingPercentage, int mapperNumber, double mapperOverlapRatio,
            long sourceRecordCoun) throws IOException {
        Path seqFilePath = new Path(outputPath, BatchConstants.CFG_STATISTICS_CUBOID_ESTIMATION_FILENAME);
        writeCuboidStatisticsInner(conf, seqFilePath, cuboidHLLMap, samplingPercentage, mapperNumber,
                mapperOverlapRatio, sourceRecordCoun);
    }

    //Be care of that the file name for partial cuboid statistics should start with BatchConstants.CFG_OUTPUT_STATISTICS,
    //Then for later statistics merging, only files starting with BatchConstants.CFG_OUTPUT_STATISTICS will be used
    public static void writePartialCuboidStatistics(Configuration conf, Path outputPath, //
            Map<Long, HLLCounter> cuboidHLLMap, int samplingPercentage, int mapperNumber, double mapperOverlapRatio,
            int shard) throws IOException {
        Path seqFilePath = new Path(outputPath, BatchConstants.CFG_OUTPUT_STATISTICS + "_" + shard);
        writeCuboidStatisticsInner(conf, seqFilePath, cuboidHLLMap, samplingPercentage, mapperNumber,
                mapperOverlapRatio, 0);
    }

    private static void writeCuboidStatisticsInner(Configuration conf, Path outputFilePath, //
            Map<Long, HLLCounter> cuboidHLLMap, int samplingPercentage, int mapperNumber, double mapperOverlapRatio,
            long sourceRecordCount) throws IOException {
        List<Long> allCuboids = Lists.newArrayList();
        allCuboids.addAll(cuboidHLLMap.keySet());
        Collections.sort(allCuboids);

        ByteBuffer valueBuf = ByteBuffer.allocate(BufferedMeasureCodec.DEFAULT_BUFFER_SIZE);
        SequenceFile.Writer writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(outputFilePath),
                SequenceFile.Writer.keyClass(LongWritable.class), SequenceFile.Writer.valueClass(BytesWritable.class));
        try {
            // mapper overlap ratio at key -1
            writer.append(new LongWritable(-1), new BytesWritable(Bytes.toBytes(mapperOverlapRatio)));

            // mapper number at key -2
            writer.append(new LongWritable(-2), new BytesWritable(Bytes.toBytes(mapperNumber)));

            // sampling percentage at key 0
            writer.append(new LongWritable(0L), new BytesWritable(Bytes.toBytes(samplingPercentage)));

            // flat table source_count at key -3
            writer.append(new LongWritable(-3), new BytesWritable(Bytes.toBytes(sourceRecordCount)));

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
