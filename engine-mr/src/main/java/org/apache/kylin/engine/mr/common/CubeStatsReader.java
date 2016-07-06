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
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.SumHelper;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.cuboid.CuboidScheduler;
import org.apache.kylin.cube.kv.CubeDimEncMap;
import org.apache.kylin.cube.kv.RowKeyEncoder;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.apache.kylin.measure.hllc.HyperLogLogPlusCounter;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * This should be in cube module. It's here in engine-mr because currently stats
 * are saved as sequence files thus a hadoop dependency.
 */
public class CubeStatsReader {

    private static final Logger logger = LoggerFactory.getLogger(CubeStatsReader.class);

    final CubeSegment seg;
    final int samplingPercentage;
    final double mapperOverlapRatioOfFirstBuild; // only makes sense for the first build, is meaningless after merge
    final Map<Long, HyperLogLogPlusCounter> cuboidRowEstimatesHLL;

    public CubeStatsReader(CubeSegment cubeSegment, KylinConfig kylinConfig) throws IOException {
        ResourceStore store = ResourceStore.getStore(kylinConfig);
        String statsKey = cubeSegment.getStatisticsResourcePath();
        File tmpSeqFile = writeTmpSeqFile(store.getResource(statsKey).inputStream);
        Reader reader = null;

        try {
            Configuration hadoopConf = HadoopUtil.getCurrentConfiguration();

            Path path = new Path(HadoopUtil.fixWindowsPath("file://" + tmpSeqFile.getAbsolutePath()));
            Option seqInput = SequenceFile.Reader.file(path);
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
                    HyperLogLogPlusCounter hll = new HyperLogLogPlusCounter(kylinConfig.getCubeStatsHLLPrecision());
                    ByteArray byteArray = new ByteArray(value.getBytes());
                    hll.readRegisters(byteArray.asBuffer());
                    counterMap.put(key.get(), hll);
                }
            }

            this.seg = cubeSegment;
            this.samplingPercentage = percentage;
            this.mapperOverlapRatioOfFirstBuild = mapperOverlapRatio;
            this.cuboidRowEstimatesHLL = counterMap;

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

    public Map<Long, Long> getCuboidRowEstimatesHLL() {
        return getCuboidRowCountMapFromSampling(cuboidRowEstimatesHLL, samplingPercentage);
    }

    // return map of Cuboid ID => MB
    public Map<Long, Double> getCuboidSizeMap() {
        return getCuboidSizeMapFromRowCount(seg, getCuboidRowEstimatesHLL());
    }

    public double getMapperOverlapRatioOfFirstBuild() {
        return mapperOverlapRatioOfFirstBuild;
    }

    public static Map<Long, Long> getCuboidRowCountMapFromSampling(Map<Long, HyperLogLogPlusCounter> hllcMap, int samplingPercentage) {
        Map<Long, Long> cuboidRowCountMap = Maps.newHashMap();
        for (Map.Entry<Long, HyperLogLogPlusCounter> entry : hllcMap.entrySet()) {
            // No need to adjust according sampling percentage. Assumption is that data set is far
            // more than cardinality. Even a percentage of the data should already see all cardinalities.
            cuboidRowCountMap.put(entry.getKey(), entry.getValue().getCountEstimate());
        }
        return cuboidRowCountMap;
    }

    public static Map<Long, Double> getCuboidSizeMapFromRowCount(CubeSegment cubeSegment, Map<Long, Long> rowCountMap) {
        final CubeDesc cubeDesc = cubeSegment.getCubeDesc();
        final List<Integer> rowkeyColumnSize = Lists.newArrayList();
        final long baseCuboidId = Cuboid.getBaseCuboidId(cubeDesc);
        final Cuboid baseCuboid = Cuboid.findById(cubeDesc, baseCuboidId);
        final List<TblColRef> columnList = baseCuboid.getColumns();
        final CubeDimEncMap dimEncMap = cubeSegment.getDimensionEncodingMap();

        for (int i = 0; i < columnList.size(); i++) {
            rowkeyColumnSize.add(dimEncMap.get(columnList.get(i)).getLengthOfEncoding());
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
        KylinConfig kylinConf = cubeSegment.getConfig();

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
            if (measureDesc.getFunction().getMeasureType().isMemoryHungry()) {
                isMemoryHungry = true;
            }
            DataType returnType = measureDesc.getFunction().getReturnDataType();
            space += returnType.getStorageBytesEstimate();
        }
        bytesLength += space;

        double ret = 1.0 * bytesLength * rowCount / (1024L * 1024L);
        if (isMemoryHungry) {
            double cuboidSizeMemHungryRatio = kylinConf.getJobCuboidSizeMemHungryRatio();
            logger.info("Cube is memory hungry, storage size estimation multiply " + cuboidSizeMemHungryRatio);
            ret *= cuboidSizeMemHungryRatio;
        } else {
            double cuboidSizeRatio = kylinConf.getJobCuboidSizeRatio();
            logger.info("Cube is not memory hungry, storage size estimation multiply " + cuboidSizeRatio);
            ret *= cuboidSizeRatio;
        }
        logger.info("Cuboid " + cuboidId + " has " + rowCount + " rows, each row size is " + bytesLength + " bytes." + " Total size is " + ret + "M.");
        return ret;
    }

    private void print(PrintWriter out) {
        Map<Long, Long> cuboidRows = getCuboidRowEstimatesHLL();
        Map<Long, Double> cuboidSizes = getCuboidSizeMap();
        List<Long> cuboids = new ArrayList<Long>(cuboidRows.keySet());
        Collections.sort(cuboids);

        out.println("============================================================================");
        out.println("Statistics of " + seg);
        out.println();
        out.println("Cube statistics hll precision: " + cuboidRowEstimatesHLL.values().iterator().next().getPrecision());
        out.println("Total cuboids: " + cuboidRows.size());
        out.println("Total estimated rows: " + SumHelper.sumLong(cuboidRows.values()));
        out.println("Total estimated size(MB): " + SumHelper.sumDouble(cuboidSizes.values()));
        out.println("Sampling percentage:  " + samplingPercentage);
        out.println("Mapper overlap ratio: " + mapperOverlapRatioOfFirstBuild);
        printKVInfo(out);
        printCuboidInfoTreeEntry(cuboidRows, cuboidSizes, out);
        out.println("----------------------------------------------------------------------------");
    }

    private void printCuboidInfoTreeEntry(Map<Long, Long> cuboidRows, Map<Long, Double> cuboidSizes, PrintWriter out) {
        CubeDesc cubeDesc = seg.getCubeDesc();
        CuboidScheduler scheduler = new CuboidScheduler(cubeDesc);
        long baseCuboid = Cuboid.getBaseCuboidId(cubeDesc);
        int dimensionCount = Long.bitCount(baseCuboid);
        printCuboidInfoTree(-1L, baseCuboid, scheduler, cuboidRows, cuboidSizes, dimensionCount, 0, out);
    }

    private void printKVInfo(PrintWriter writer) {
        Cuboid cuboid = Cuboid.getBaseCuboid(seg.getCubeDesc());
        RowKeyEncoder encoder = new RowKeyEncoder(seg, cuboid);
        for (TblColRef col : cuboid.getColumns()) {
            writer.println("Length of dimension " + col + " is " + encoder.getColumnLength(col));
        }
    }

    private static void printCuboidInfoTree(long parent, long cuboidID, final CuboidScheduler scheduler, Map<Long, Long> cuboidRows, Map<Long, Double> cuboidSizes, int dimensionCount, int depth, PrintWriter out) {
        printOneCuboidInfo(parent, cuboidID, cuboidRows, cuboidSizes, dimensionCount, depth, out);

        List<Long> children = scheduler.getSpanningCuboid(cuboidID);
        Collections.sort(children);

        for (Long child : children) {
            printCuboidInfoTree(cuboidID, child, scheduler, cuboidRows, cuboidSizes, dimensionCount, depth + 1, out);
        }
    }

    private static void printOneCuboidInfo(long parent, long cuboidID, Map<Long, Long> cuboidRows, Map<Long, Double> cuboidSizes, int dimensionCount, int depth, PrintWriter out) {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < depth; i++) {
            sb.append("    ");
        }
        String cuboidName = Cuboid.getDisplayName(cuboidID, dimensionCount);
        sb.append("|---- Cuboid ").append(cuboidName);

        long rowCount = cuboidRows.get(cuboidID);
        double size = cuboidSizes.get(cuboidID);
        sb.append(", est row: ").append(rowCount).append(", est MB: ").append(formatDouble(size));

        if (parent != -1) {
            sb.append(", shrink: ").append(formatDouble(100.0 * cuboidRows.get(cuboidID) / cuboidRows.get(parent))).append("%");
        }

        out.println(sb.toString());
    }

    private static String formatDouble(double input) {
        return new DecimalFormat("#.##").format(input);
    }

    public static void main(String[] args) throws IOException {
        System.out.println("CubeStatsReader is used to read cube statistic saved in metadata store");
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
