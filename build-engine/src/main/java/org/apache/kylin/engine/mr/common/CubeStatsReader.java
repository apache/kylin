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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
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
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.SumHelper;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.cuboid.CuboidScheduler;
import org.apache.kylin.cube.kv.RowKeyEncoder;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.measure.hllc.HLLCounter;
import org.apache.kylin.measure.topn.TopNMeasureType;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.FunctionDesc;
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
    final int mapperNumberOfFirstBuild; // becomes meaningless after merge
    final double mapperOverlapRatioOfFirstBuild; // becomes meaningless after merge
    final Map<Long, HLLCounter> cuboidRowEstimatesHLL;
    final CuboidScheduler cuboidScheduler;
    public final long sourceRowCount;

    public CubeStatsReader(CubeSegment cubeSegment, KylinConfig kylinConfig) throws IOException {
        this(cubeSegment, cubeSegment.getCuboidScheduler(), kylinConfig);
    }

    /**
     * @param cuboidScheduler if it's null, part of it's functions will not be supported
     */
    public CubeStatsReader(CubeSegment cubeSegment, CuboidScheduler cuboidScheduler, KylinConfig kylinConfig)
            throws IOException {
        this.seg = cubeSegment;
        this.cuboidScheduler = cuboidScheduler;
        ResourceStore store = ResourceStore.getStore(kylinConfig);
        String statsKey = cubeSegment.getStatisticsResourcePath();
        RawResource resource = store.getResource(statsKey);
        if (resource != null) {
            File tmpSeqFile = writeTmpSeqFile(resource.content());
            Path path = new Path(HadoopUtil.fixWindowsPath("file://" + tmpSeqFile.getAbsolutePath()));
            logger.info("Reading statistics from {}", path);
            CubeStatsResult cubeStatsResult = new CubeStatsResult(path, kylinConfig.getCubeStatsHLLPrecision());
            tmpSeqFile.delete();

            this.samplingPercentage = cubeStatsResult.getPercentage();
            this.mapperNumberOfFirstBuild = cubeStatsResult.getMapperNumber();
            this.mapperOverlapRatioOfFirstBuild = cubeStatsResult.getMapperOverlapRatio();
            this.cuboidRowEstimatesHLL = cubeStatsResult.getCounterMap();
            this.sourceRowCount = cubeStatsResult.getSourceRecordCount();
        } else {
            // throw new IllegalStateException("Missing resource at " + statsKey);
            logger.warn("{} is not exists.", statsKey);
            this.samplingPercentage = -1;
            this.mapperNumberOfFirstBuild = -1;
            this.mapperOverlapRatioOfFirstBuild = -1.0;
            this.cuboidRowEstimatesHLL = null;
            this.sourceRowCount = -1L;
        }
    }

    /**
     * Read statistics from
     * @param path
     * rather than
     * @param cubeSegment
     *
     * Since the statistics are from
     * @param path
     * cuboid scheduler should be provided by default
     */
    public CubeStatsReader(CubeSegment cubeSegment, CuboidScheduler cuboidScheduler, KylinConfig kylinConfig, Path path)
            throws IOException {
        CubeStatsResult cubeStatsResult = new CubeStatsResult(path, kylinConfig.getCubeStatsHLLPrecision());

        this.seg = cubeSegment;
        this.cuboidScheduler = cuboidScheduler;
        this.samplingPercentage = cubeStatsResult.getPercentage();
        this.mapperNumberOfFirstBuild = cubeStatsResult.getMapperNumber();
        this.mapperOverlapRatioOfFirstBuild = cubeStatsResult.getMapperOverlapRatio();
        this.cuboidRowEstimatesHLL = cubeStatsResult.getCounterMap();
        this.sourceRowCount = cubeStatsResult.getSourceRecordCount();
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

    public Map<Long, HLLCounter> getCuboidRowHLLCounters() {
        return this.cuboidRowEstimatesHLL;
    }

    public int getSamplingPercentage() {
        return samplingPercentage;
    }

    public long getSourceRowCount() {
        return sourceRowCount;
    }

    public Map<Long, Long> getCuboidRowEstimatesHLL() {
        if (cuboidRowEstimatesHLL == null) {
            return null;
        }
        return getCuboidRowCountMapFromSampling(cuboidRowEstimatesHLL, samplingPercentage);
    }

    public Map<Long, Double> getCuboidSizeMap() {
        return getCuboidSizeMap(false);
    }

    public Map<Long, Double> getCuboidSizeMap(boolean origin) {
        return getCuboidSizeMapFromRowCount(seg, getCuboidRowEstimatesHLL(), sourceRowCount, origin);
    }

    public double estimateCubeSize() {
        return SumHelper.sumDouble(getCuboidSizeMap().values());
    }

    public int getMapperNumberOfFirstBuild() {
        return mapperNumberOfFirstBuild;
    }

    public double getMapperOverlapRatioOfFirstBuild() {
        return mapperOverlapRatioOfFirstBuild;
    }

    public static Map<Long, Long> getCuboidRowCountMapFromSampling(Map<Long, HLLCounter> hllcMap,
            int samplingPercentage) {
        Map<Long, Long> cuboidRowCountMap = Maps.newHashMap();
        for (Map.Entry<Long, HLLCounter> entry : hllcMap.entrySet()) {
            // No need to adjust according sampling percentage. Assumption is that data set is far
            // more than cardinality. Even a percentage of the data should already see all cardinalities.
            cuboidRowCountMap.put(entry.getKey(), entry.getValue().getCountEstimate());
        }
        return cuboidRowCountMap;
    }

    public static Map<Long, Double> getCuboidSizeMapFromRowCount(CubeSegment cubeSegment, Map<Long, Long> rowCountMap,
            long sourceRowCount) {
        return getCuboidSizeMapFromRowCount(cubeSegment, rowCountMap, sourceRowCount, true);
    }

    private static Map<Long, Double> getCuboidSizeMapFromRowCount(CubeSegment cubeSegment, Map<Long, Long> rowCountMap,
                                                                  long sourceRowCount, boolean origin) {
        final CubeDesc cubeDesc = cubeSegment.getCubeDesc();
        final List<Integer> rowkeyColumnSize = Lists.newArrayList();
        final Cuboid baseCuboid = Cuboid.getBaseCuboid(cubeDesc);
        final List<TblColRef> columnList = baseCuboid.getColumns();
        final Long baseCuboidRowCount = rowCountMap.get(baseCuboid.getId());

        for (int i = 0; i < columnList.size(); i++) {
            /*
             * A workaround, for the fact kylin do not support self-defined encode in Kylin 4,
             * it is done by Parquet(https://github.com/apache/parquet-format/blob/master/Encodings.md) for Kylin 4.
             * It's complex and hard to calculate real size for specific literal value, so I propose to use 4 for a rough estimation.
             */
            rowkeyColumnSize.add(4);
        }

        Map<Long, Double> sizeMap = Maps.newHashMap();
        for (Map.Entry<Long, Long> entry : rowCountMap.entrySet()) {
            sizeMap.put(entry.getKey(), estimateCuboidStorageSize(cubeSegment, entry.getKey(), entry.getValue(),
                    baseCuboid.getId(), baseCuboidRowCount, rowkeyColumnSize, sourceRowCount));
        }

        if (!origin && cubeSegment.getConfig().enableJobCuboidSizeOptimize()) {
            optimizeSizeMap(sizeMap, cubeSegment);
        }

        return sizeMap;
    }

    private static Double harmonicMean(List<Double> data) {
        if (data == null || data.size() == 0) {
            return 1.0;
        }
        Double sum = 0.0;
        for (Double item : data) {
            sum += 1.0 / item;
        }
        return data.size() / sum;
    }

    private static List<Double> getHistoricalRating(CubeSegment cubeSegment,
                                                    CubeInstance cubeInstance,
                                                    int totalLevels) {
        boolean isMerged = cubeSegment.isMerged();

        Map<Integer, List<Double>> layerRatio = Maps.newHashMap();
        List<Double> result = Lists.newArrayList();

        for (CubeSegment seg : cubeInstance.getSegments(SegmentStatusEnum.READY)) {
            if (seg.isMerged() != isMerged || seg.getEstimateRatio() == null) {
                continue;
            }

            logger.info("get ratio from {} with: {}", seg.getName(), StringUtils.join(seg.getEstimateRatio(), ","));

            for(int level = 0; level <= totalLevels; level++) {
                if (seg.getEstimateRatio().get(level) <= 0) {
                    continue;
                }

                List<Double> temp = layerRatio.get(level) == null ? Lists.newArrayList() : layerRatio.get(level);

                temp.add(seg.getEstimateRatio().get(level));
                layerRatio.put(level, temp);
            }
        }

        if (layerRatio.size() == 0) {
            logger.info("Fail to get historical rating.");
            return null;
        } else {
            for(int level = 0; level <= totalLevels; level++) {
                logger.debug("level {}: {}", level, StringUtils.join(layerRatio.get(level), ","));
                result.add(level, harmonicMean(layerRatio.get(level)));
            }

            logger.info("Finally estimate ratio is {}", StringUtils.join(result, ","));

            return result;
        }
    }

    private static void optimizeSizeMap(Map<Long, Double> sizeMap, CubeSegment cubeSegment) {
        CubeInstance cubeInstance = cubeSegment.getCubeInstance();
        int totalLevels = cubeInstance.getCuboidScheduler().getBuildLevel();
        List<List<Long>> layeredCuboids = cubeInstance.getCuboidScheduler().getCuboidsByLayer();

        logger.info("cube size is {} before optimize", SumHelper.sumDouble(sizeMap.values()));

        List<Double> levelRating = getHistoricalRating(cubeSegment, cubeInstance, totalLevels);

        if (levelRating == null) {
            logger.info("Fail to optimize, use origin.");
            return;
        }

        for (int level = 0; level <= totalLevels; level++) {
            Double rate = levelRating.get(level);

            for (Long cuboidId : layeredCuboids.get(level)) {
                double oriValue = (sizeMap.get(cuboidId) == null ? 0.0 : sizeMap.get(cuboidId));
                sizeMap.put(cuboidId, oriValue * rate);
            }
        }

        logger.info("cube size is {} after optimize", SumHelper.sumDouble(sizeMap.values()));

        return;
    }


    /**
     * Estimate the cuboid's size
     *
     * @return the cuboid size in M bytes
     */
    private static double estimateCuboidStorageSize(CubeSegment cubeSegment, long cuboidId, long rowCount,
            long baseCuboidId, long baseCuboidCount, List<Integer> rowKeyColumnLength, long sourceRowCount) {

        int rowkeyLength = cubeSegment.getRowKeyPreambleSize();
        KylinConfig kylinConf = cubeSegment.getConfig();

        long mask = Long.highestOneBit(baseCuboidId);
        long parentCuboidIdActualLength = (long) Long.SIZE - Long.numberOfLeadingZeros(baseCuboidId);
        for (int i = 0; i < parentCuboidIdActualLength; i++) {
            if ((mask & cuboidId) > 0) {
                rowkeyLength += rowKeyColumnLength.get(i); //colIO.getColumnLength(columnList.get(i));
            }
            mask = mask >> 1;
        }

        // add the measure length
        int normalSpace = rowkeyLength;
        int countDistinctSpace = 0;
        double percentileSpace = 0;
        int topNSpace = 0;
        for (MeasureDesc measureDesc : cubeSegment.getCubeDesc().getMeasures()) {
            if (rowCount == 0)
                break;
            DataType returnType = measureDesc.getFunction().getReturnDataType();
            if (measureDesc.getFunction().getExpression().equals(FunctionDesc.FUNC_COUNT_DISTINCT)) {
                long estimateDistinctCount = sourceRowCount / rowCount;
                estimateDistinctCount = estimateDistinctCount == 0 ? 1L : estimateDistinctCount;
                countDistinctSpace += returnType.getStorageBytesEstimate(estimateDistinctCount);
            } else if (measureDesc.getFunction().getExpression().equals(FunctionDesc.FUNC_PERCENTILE)) {
                percentileSpace += returnType.getStorageBytesEstimate(baseCuboidCount * 1.0 / rowCount);
            } else if (measureDesc.getFunction().getExpression().equals(TopNMeasureType.FUNC_TOP_N)) {
                long estimateTopNCount = sourceRowCount / rowCount;
                estimateTopNCount = estimateTopNCount == 0 ? 1L : estimateTopNCount;
                topNSpace += returnType.getStorageBytesEstimate(estimateTopNCount);
            } else {
                normalSpace += returnType.getStorageBytesEstimate();
            }
        }

        double cuboidSizeMemHungryRatio = kylinConf.getJobCuboidSizeCountDistinctRatio();
        double cuboidSizeTopNRatio = kylinConf.getJobCuboidSizeTopNRatio();

        double ret = (1.0 * normalSpace * rowCount
                + 1.0 * countDistinctSpace * rowCount * cuboidSizeMemHungryRatio + 1.0 * percentileSpace * rowCount
                + 1.0 * topNSpace * rowCount * cuboidSizeTopNRatio) / (1024L * 1024L);
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
        out.println(
                "Cube statistics hll precision: " + cuboidRowEstimatesHLL.values().iterator().next().getPrecision());
        out.println("Total cuboids: " + cuboidRows.size());
        out.println("Total estimated rows: " + SumHelper.sumLong(cuboidRows.values()));
        out.println("Total estimated size(MB): " + SumHelper.sumDouble(cuboidSizes.values()));
        out.println("Sampling percentage:  " + samplingPercentage);
        out.println("Mapper overlap ratio: " + mapperOverlapRatioOfFirstBuild);
        out.println("Mapper number: " + mapperNumberOfFirstBuild);
        printKVInfo(out);
        printCuboidInfoTreeEntry(cuboidRows, cuboidSizes, out);
        out.println("----------------------------------------------------------------------------");
    }

    //return MB
    public double estimateLayerSize(int level) {
        if (cuboidScheduler == null) {
            throw new UnsupportedOperationException("cuboid scheduler is null");
        }
        List<List<Long>> layeredCuboids = cuboidScheduler.getCuboidsByLayer();
        Map<Long, Double> cuboidSizeMap = getCuboidSizeMap();
        double ret = 0;
        for (Long cuboidId : layeredCuboids.get(level)) {
            ret += cuboidSizeMap.get(cuboidId) == null ? 0.0 : cuboidSizeMap.get(cuboidId);
        }

        logger.info("Estimating size for layer {}, all cuboids are {}, total size is {}", level,
                StringUtils.join(layeredCuboids.get(level), ","), ret);
        return ret;
    }

    public List<Long> getCuboidsByLayer(int level) {
        if (cuboidScheduler == null) {
            throw new UnsupportedOperationException("cuboid scheduler is null");
        }
        List<List<Long>> layeredCuboids = cuboidScheduler.getCuboidsByLayer();
        return layeredCuboids.get(level);
    }

    private void printCuboidInfoTreeEntry(Map<Long, Long> cuboidRows, Map<Long, Double> cuboidSizes, PrintWriter out) {
        if (cuboidScheduler == null) {
            throw new UnsupportedOperationException("cuboid scheduler is null");
        }
        long baseCuboid = Cuboid.getBaseCuboidId(seg.getCubeDesc());
        int dimensionCount = Long.bitCount(baseCuboid);
        printCuboidInfoTree(-1L, baseCuboid, cuboidScheduler, cuboidRows, cuboidSizes, dimensionCount, 0, out);
    }

    private void printKVInfo(PrintWriter writer) {
        Cuboid cuboid = Cuboid.getBaseCuboid(seg.getCubeDesc());
        RowKeyEncoder encoder = new RowKeyEncoder(seg, cuboid);
        for (TblColRef col : cuboid.getColumns()) {
            writer.println("Length of dimension " + col + " is " + encoder.getColumnLength(col));
        }
    }

    private static void printCuboidInfoTree(long parent, long cuboidID, final CuboidScheduler scheduler,
            Map<Long, Long> cuboidRows, Map<Long, Double> cuboidSizes, int dimensionCount, int depth, PrintWriter out) {
        printOneCuboidInfo(parent, cuboidID, cuboidRows, cuboidSizes, dimensionCount, depth, out);

        List<Long> children = scheduler.getSpanningCuboid(cuboidID);
        Collections.sort(children);

        for (Long child : children) {
            printCuboidInfoTree(cuboidID, child, scheduler, cuboidRows, cuboidSizes, dimensionCount, depth + 1, out);
        }
    }

    private static void printOneCuboidInfo(long parent, long cuboidID, Map<Long, Long> cuboidRows,
            Map<Long, Double> cuboidSizes, int dimensionCount, int depth, PrintWriter out) {
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
            sb.append(", shrink: ").append(formatDouble(100.0 * cuboidRows.get(cuboidID) / cuboidRows.get(parent)))
                    .append("%");
        }

        out.println(sb.toString());
    }

    private static String formatDouble(double input) {
        return new DecimalFormat("#.##", DecimalFormatSymbols.getInstance(Locale.ROOT)).format(input);
    }

    public static class CubeStatsResult {
        private int percentage = 100;
        private double mapperOverlapRatio = 0;
        private long sourceRecordCount = 0;
        private int mapperNumber = 0;
        private Map<Long, HLLCounter> counterMap = Maps.newHashMap();

        public CubeStatsResult(Path path, int precision) throws IOException {
            Configuration hadoopConf = HadoopUtil.getCurrentConfiguration();
            Option seqInput = SequenceFile.Reader.file(path);
            try (Reader reader = new SequenceFile.Reader(hadoopConf, seqInput)) {
                LongWritable key = (LongWritable) ReflectionUtils.newInstance(reader.getKeyClass(), hadoopConf);
                BytesWritable value = (BytesWritable) ReflectionUtils.newInstance(reader.getValueClass(), hadoopConf);
                while (reader.next(key, value)) {
                    if (key.get() == 0L) {
                        percentage = Bytes.toInt(value.getBytes());
                    } else if (key.get() == -1) {
                        mapperOverlapRatio = Bytes.toDouble(value.getBytes());
                    } else if (key.get() == -2) {
                        mapperNumber = Bytes.toInt(value.getBytes());
                    } else if (key.get() == -3) {
                        sourceRecordCount = Bytes.toLong(value.getBytes());
                    } else if (key.get() > 0) {
                        HLLCounter hll = new HLLCounter(precision);
                        ByteArray byteArray = new ByteArray(value.getBytes());
                        hll.readRegisters(byteArray.asBuffer());
                        counterMap.put(key.get(), hll);
                    }
                }
            }
        }

        public int getPercentage() {
            return percentage;
        }

        public double getMapperOverlapRatio() {
            return mapperOverlapRatio;
        }

        public int getMapperNumber() {
            return mapperNumber;
        }

        public Map<Long, HLLCounter> getCounterMap() {
            return Collections.unmodifiableMap(counterMap);
        }

        public long getSourceRecordCount() {
            return sourceRecordCount;
        }
    }

    public static void main(String[] args) throws IOException {
        System.out.println("CubeStatsReader is used to read cube statistic saved in metadata store");
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        CubeInstance cube = CubeManager.getInstance(config).getCube(args[0]);
        List<CubeSegment> segments = cube.getSegments();

        PrintWriter out = new PrintWriter(
                new BufferedWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8)));
        for (CubeSegment seg : segments) {
            try {
                new CubeStatsReader(seg, config).print(out);
            } catch (Exception e) {
                logger.info("CubeStatsReader for Segment {} failed, skip it.", seg.getName());
            }
        }
        out.flush();
    }

}
