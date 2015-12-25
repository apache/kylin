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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Reader.Option;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.hll.HyperLogLogPlusCounter;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.apache.kylin.engine.mr.steps.InMemCuboidJob;
import org.apache.kylin.metadata.model.DataType;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * This should be in cube module. It's here in engine-mr because currently stats
 * are saved as sequence files thus a hadoop dependency.
 */
public class CubeStatsReader {

    private static final Logger logger = LoggerFactory.getLogger(InMemCuboidJob.class);

    final CubeSegment seg;
    final int samplingPercentage;
    final double mapperOverlapRatioOfFirstBuild; // only makes sense for the first build, is meaningless after merge
    final Map<Long, HyperLogLogPlusCounter> cuboidRowCountMap;

    public CubeStatsReader(CubeSegment cubeSegment, KylinConfig kylinConfig) throws IOException {
        ResourceStore store = ResourceStore.getStore(kylinConfig);
        String statsKey = cubeSegment.getStatisticsResourcePath();
        File tmpSeqFile = writeTmpSeqFile(store.getResource(statsKey).inputStream);
        Reader reader = null;

        try {
            Configuration hadoopConf = HadoopUtil.getCurrentConfiguration();

            Option seqInput = SequenceFile.Reader.file(new Path("file://" + tmpSeqFile.getAbsolutePath()));
            reader = new SequenceFile.Reader(hadoopConf, seqInput);

            int percentage = 100;
            double mapperOverlapRatio = 0;
            Map<Long, HyperLogLogPlusCounter> counterMap = Maps.newHashMap();

            LongWritable key = (LongWritable) ReflectionUtils.newInstance(reader.getKeyClass(), hadoopConf);
            BytesWritable value = (BytesWritable) ReflectionUtils.newInstance(reader.getValueClass(), hadoopConf);
            while (reader.next(key, value)) {
                if (key.get() == 0L) {
                    percentage = Bytes.toInt(value.getBytes());
                } else if (key.get() == -1) {
                    mapperOverlapRatio = Bytes.toDouble(value.getBytes());
                } else {
                    HyperLogLogPlusCounter hll = new HyperLogLogPlusCounter(14);
                    ByteArray byteArray = new ByteArray(value.getBytes());
                    hll.readRegisters(byteArray.asBuffer());
                    counterMap.put(key.get(), hll);
                }
            }

            this.seg = cubeSegment;
            this.samplingPercentage = percentage;
            this.mapperOverlapRatioOfFirstBuild = mapperOverlapRatio;
            this.cuboidRowCountMap = counterMap;

        } finally {
            IOUtils.closeStream(reader);
            tmpSeqFile.delete();
        }
    }

    private File writeTmpSeqFile(InputStream inputStream) throws IOException {
        File tempFile = File.createTempFile("kylin_stats_tmp", ".seq");
        FileOutputStream out = null;
        try {
            out = new FileOutputStream(tempFile);
            org.apache.commons.io.IOUtils.copy(inputStream, out);
        } finally {
            IOUtils.closeStream(inputStream);
            IOUtils.closeStream(out);
        }
        return tempFile;
    }

    public Map<Long, Long> getCuboidRowCountMap() {
        return getCuboidRowCountMapFromSampling(cuboidRowCountMap, samplingPercentage);
    }

    // return map of Cuboid ID => MB
    public Map<Long, Double> getCuboidSizeMap() {
        return getCuboidSizeMapFromRowCount(seg, getCuboidRowCountMap());
    }
    
    public double getMapperOverlapRatioOfFirstBuild() {
        return mapperOverlapRatioOfFirstBuild;
    }

    public void print(PrintWriter out) {
        Map<Long, Long> rowCountMap = getCuboidRowCountMap();
        Map<Long, Double> sizeMap = getCuboidSizeMap();
        List<Long> cuboids = new ArrayList<Long>(rowCountMap.keySet());
        Collections.sort(cuboids);

        out.println("============================================================================");
        out.println("Statistics of " + seg);
        out.println("  Sampling percentage:  " + samplingPercentage);
        out.println("  Mapper overlap ratio: " + mapperOverlapRatioOfFirstBuild);
        for (Long cuboid : cuboids) {
            out.println("  Cuboid :\t" + rowCountMap.get(cuboid) + " rows, " + sizeMap.get(cuboid) + " MB");
        }
        out.println("----------------------------------------------------------------------------");
    }

    public static Map<Long, Long> getCuboidRowCountMapFromSampling(Map<Long, HyperLogLogPlusCounter> hllcMap, int samplingPercentage) {
        return Maps.transformValues(hllcMap, new Function<HyperLogLogPlusCounter, Long>() {
            @Nullable
            @Override
            public Long apply(HyperLogLogPlusCounter input) {
                // No need to adjust according sampling percentage. Assumption is that data set is far
                // more than cardinality. Even a percentage of the data should already see all cardinalities.
                return input.getCountEstimate();
            }
        });
    }

    public static Map<Long, Double> getCuboidSizeMapFromRowCount(CubeSegment cubeSegment, Map<Long, Long> rowCountMap) {
        final CubeDesc cubeDesc = cubeSegment.getCubeDesc();
        final List<Integer> rowkeyColumnSize = Lists.newArrayList();
        final long baseCuboidId = Cuboid.getBaseCuboidId(cubeDesc);
        final Cuboid baseCuboid = Cuboid.findById(cubeDesc, baseCuboidId);
        final List<TblColRef> columnList = baseCuboid.getColumns();

        for (int i = 0; i < columnList.size(); i++) {
            rowkeyColumnSize.add(cubeSegment.getColumnLength(columnList.get(i)));
        }

        Map<Long, Double> sizeMap = Maps.newHashMap();
        for (Map.Entry<Long, Long> entry : rowCountMap.entrySet()) {
            sizeMap.put(entry.getKey(), estimateCuboidStorageSize(cubeSegment, entry.getKey(), entry.getValue(), baseCuboidId, rowkeyColumnSize));
        }
        return sizeMap;
    }

    /**
     * Estimate the cuboid's size
     *
     * @return the cuboid size in M bytes
     */
    private static double estimateCuboidStorageSize(CubeSegment cubeSegment, long cuboidId, long rowCount, long baseCuboidId, List<Integer> rowKeyColumnLength) {

        int bytesLength = cubeSegment.getRowKeyPreambleSize();

        long mask = Long.highestOneBit(baseCuboidId);
        long parentCuboidIdActualLength = Long.SIZE - Long.numberOfLeadingZeros(baseCuboidId);
        for (int i = 0; i < parentCuboidIdActualLength; i++) {
            if ((mask & cuboidId) > 0) {
                bytesLength += rowKeyColumnLength.get(i); //colIO.getColumnLength(columnList.get(i));
            }
            mask = mask >> 1;
        }

        // add the measure length
        int space = 0;
        boolean isMemoryHungry = false;
        for (MeasureDesc measureDesc : cubeSegment.getCubeDesc().getMeasures()) {
            if (measureDesc.getFunction().isCountDistinct()) {
                isMemoryHungry = true;
            }
            DataType returnType = measureDesc.getFunction().getReturnDataType();
            space += returnType.getStorageBytesEstimate();
        }
        bytesLength += space;

        double ret = 1.0 * bytesLength * rowCount / (1024L * 1024L);
        if (isMemoryHungry) {
            logger.info("Cube is memory hungry, storage size estimation multiply 0.05");
            ret *= 0.05;
        } else {
            logger.info("Cube is not memory hungry, storage size estimation multiply 0.25");
            ret *= 0.25;
        }
        logger.info("Cuboid " + cuboidId + " has " + rowCount + " rows, each row size is " + bytesLength + " bytes." + " Total size is " + ret + "M.");
        return ret;
    }

    public static void main(String[] args) throws IOException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        CubeInstance cube = CubeManager.getInstance(config).getCube(args[0]);
        List<CubeSegment> segments = cube.getSegments(SegmentStatusEnum.READY);

        PrintWriter out = new PrintWriter(System.out);
        for (CubeSegment seg : segments) {
            new CubeStatsReader(seg, config).print(out);
        }
        out.flush();
    }

}
