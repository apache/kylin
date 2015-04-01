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

package org.apache.kylin.job.hadoop.cube;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.hll.HyperLogLogPlusCounter;
import org.apache.kylin.common.mr.KylinReducer;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.estimation.CubeSizeEstimationCLI;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.cube.kv.RowKeyColumnIO;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.job.constant.BatchConstants;
import org.apache.kylin.job.hadoop.AbstractHadoopJob;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TblColRef;

import java.io.IOException;
import java.util.*;

/**
 * @author yangli9
 */
public class FactDistinctColumnsReducer extends KylinReducer<LongWritable, Text, NullWritable, Text> {

    private List<TblColRef> columnList = new ArrayList<TblColRef>();
    private boolean collectStatistics = false;
    private String statisticsOutput = null;
    private List<Long> rowKeyCountInMappers;
    private Map<Long, Long> rowKeyCountInCuboids;
    protected Map<Long, HyperLogLogPlusCounter> cuboidHLLMap = null;
    protected long baseCuboidId;

    protected CubeDesc cubeDesc;

    private RowKeyColumnIO colIO;
    public static final int SMALL_CUT = 2;  //  5 GB per region
    public static final int MEDIUM_CUT = 10; //  10 GB per region
    public static final int LARGE_CUT = 50; // 50 GB per region

    public static final int MAX_REGION = 1000;

    @Override
    protected void setup(Context context) throws IOException {
        super.bindCurrentConfiguration(context.getConfiguration());

        Configuration conf = context.getConfiguration();
        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();
        String cubeName = conf.get(BatchConstants.CFG_CUBE_NAME);
        CubeInstance cube = CubeManager.getInstance(config).getCube(cubeName);
        cubeDesc = cube.getDescriptor();

        baseCuboidId = Cuboid.getBaseCuboidId(cubeDesc);
        Cuboid baseCuboid = Cuboid.findById(cubeDesc, baseCuboidId);
        columnList = baseCuboid.getColumns();
        collectStatistics = Boolean.parseBoolean(conf.get(BatchConstants.CFG_STATISTICS_ENABLED));
        statisticsOutput = conf.get(BatchConstants.CFG_STATISTICS_OUTPUT);

        if (collectStatistics) {
            rowKeyCountInMappers = Lists.newArrayList();
            rowKeyCountInCuboids = Maps.newHashMap();
            cuboidHLLMap = Maps.newHashMap();
            String segmentName = context.getConfiguration().get(BatchConstants.CFG_CUBE_SEGMENT_NAME);
            CubeSegment cubeSegment = cube.getSegment(segmentName, SegmentStatusEnum.NEW);
            colIO = new RowKeyColumnIO(cubeSegment);
        }
    }

    @Override
    public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        if (key.get() >= 0) {
            TblColRef col = columnList.get((int) key.get());

            HashSet<ByteArray> set = new HashSet<ByteArray>();
            for (Text textValue : values) {
                ByteArray value = new ByteArray(Bytes.copy(textValue.getBytes(), 0, textValue.getLength()));
                set.add(value);
            }

            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);
            String outputPath = conf.get(BatchConstants.OUTPUT_PATH);
            FSDataOutputStream out = fs.create(new Path(outputPath, col.getName()));

            try {
                for (ByteArray value : set) {
                    out.write(value.array(), value.offset(), value.length());
                    out.write('\n');
                }
            } finally {
                out.close();
            }
        } else {
            // for hll
            long cuboidId = 0 - key.get();

            for (Text value : values) {
                HyperLogLogPlusCounter hll = new HyperLogLogPlusCounter(16);
                ByteArray byteArray = new ByteArray(value.getBytes());
                hll.readRegisters(byteArray.asBuffer());

                if (cuboidId > baseCuboidId) {
                    // if this is the summary info from a mapper, record the number before merge
                    rowKeyCountInMappers.add(hll.getCountEstimate());
                }

                if (cuboidHLLMap.get(cuboidId) != null) {
                    cuboidHLLMap.get(cuboidId).merge(hll);
                } else {
                    cuboidHLLMap.put(cuboidId, hll);
                }
            }
        }

    }

    protected void cleanup(Reducer.Context context) throws IOException, InterruptedException {

        //output the hll info;
        if (collectStatistics) {
            for (Long cuboidId : cuboidHLLMap.keySet()) {
                rowKeyCountInCuboids.put(cuboidId, cuboidHLLMap.get(cuboidId).getCountEstimate());
            }

            writeMapperAndCuboidStatistics(context);
            // writeCuboidStatistics(context);
            estimateReducers(context);
        }

    }

    private void writeCuboidStatistics(Reducer.Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        Path seqFilePath = new Path(statisticsOutput, BatchConstants.CFG_STATISTICS_CUBOID_ESTIMATION);
        SequenceFile.Writer writer = SequenceFile.createWriter(conf,
                SequenceFile.Writer.file(seqFilePath), SequenceFile.Writer.keyClass(LongWritable.class),
                SequenceFile.Writer.valueClass(LongWritable.class));

        List<Long> allCuboids = new ArrayList<Long>();
        allCuboids.addAll(rowKeyCountInCuboids.keySet());
        Collections.sort(allCuboids);
        try {
            for (long i : allCuboids) {
                writer.append(new LongWritable(i), new LongWritable(rowKeyCountInCuboids.get(i)));
            }
        } finally {
            writer.close();
        }

    }

    private void writeMapperAndCuboidStatistics(Reducer.Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        FSDataOutputStream out = fs.create(new Path(statisticsOutput, BatchConstants.CFG_STATISTICS_CUBE_ESTIMATION));

        try {
            long totalSum = 0;
            String msg;
            for (int i = 0; i < rowKeyCountInMappers.size(); i++) {
                msg = "Cube segment in Mapper " + i + " has " + rowKeyCountInMappers.get(i) + " rows.";
                totalSum += rowKeyCountInMappers.get(i);
                out.write(msg.getBytes());
                out.write('\n');
            }

            msg = "Sum of the cube segments is " + totalSum;
            out.write(msg.getBytes());
            out.write('\n');


            long grantTotal = rowKeyCountInCuboids.get(baseCuboidId + 1);
            msg = "The merged cube has " + grantTotal + " rows.";
            out.write(msg.getBytes());
            out.write('\n');

            msg = "The compaction rate is " + (grantTotal) + "/" + totalSum + " = " + (grantTotal * 100.0) / totalSum + "%.";
            out.write(msg.getBytes());
            out.write('\n');
            out.write('\n');

            List<Long> allCuboids = new ArrayList<Long>();
            allCuboids.addAll(rowKeyCountInCuboids.keySet());
            Collections.sort(allCuboids);
            for (long i : allCuboids) {
                if (i <= baseCuboidId) {
                    msg = "Cuboid " + i + " has " + rowKeyCountInCuboids.get(i) + " rows.";
                } else {
                    msg = "Totally the cube has " + rowKeyCountInCuboids.get(i) + " rows.";
                }
                out.write(msg.getBytes());
                out.write('\n');
            }

        } finally {
            out.close();
        }
    }


    @SuppressWarnings("deprecation")
    protected void estimateReducers(Reducer.Context context) throws IOException {

        DataModelDesc.RealizationCapacity cubeCapacity = cubeDesc.getModel().getCapacity();
        int cut;
        switch (cubeCapacity) {
            case SMALL:
                cut = SMALL_CUT;
                break;
            case MEDIUM:
                cut = MEDIUM_CUT;
                break;
            case LARGE:
                cut = LARGE_CUT;
                break;
            default:
                cut = SMALL_CUT;
        }

        System.out.println("Chosen cut for htable is " + cut);

        Map<Long, Long> cuboidSizeMap = Maps.newHashMap();
        long baseCuboidId = Cuboid.getBaseCuboidId(cubeDesc);
        long totalSizeInM = 0;

        List<Long> allCuboids = new ArrayList<Long>();
        allCuboids.addAll(rowKeyCountInCuboids.keySet());
        Collections.sort(allCuboids);
        for (long i : allCuboids) {
            if (i <= baseCuboidId) {
                long cuboidSize = estimateCuboidStorageSize(i, rowKeyCountInCuboids.get(i));
                cuboidSizeMap.put(i, cuboidSize);
                totalSizeInM += cuboidSize;
            }
        }

        int nRegion = Math.round((float) totalSizeInM / ((float) cut) * 1);
        nRegion = Math.max(1, nRegion);
        nRegion = Math.min(MAX_REGION, nRegion);

        int gbPerRegion = (int) (totalSizeInM / (nRegion * 1));
        gbPerRegion = Math.max(1, gbPerRegion);

        System.out.println(nRegion + " regions");
        System.out.println(gbPerRegion + " GB per region");

        List<Long> regionSplit = Lists.newArrayList();

        List<Long> allCuboidIds = Lists.newArrayList();
        allCuboidIds.addAll(cuboidSizeMap.keySet());
        Collections.sort(allCuboidIds);

        long size = 0;
        int regionIndex = 0;
        for (long cuboidId : allCuboidIds) {
            size += cuboidSizeMap.get(cuboidId);
            if (size >= gbPerRegion * 1) {
                regionSplit.add(cuboidId);
                System.out.println("Region " + regionIndex + " will be " + size + " MB, contains cuboid " + cuboidId);
                size = 0;
            }
        }


        Configuration conf = context.getConfiguration();
        Path seqFilePath = new Path(statisticsOutput, "part-r-00000");
        SequenceFile.Writer writer = SequenceFile.createWriter(conf,
                SequenceFile.Writer.file(seqFilePath), SequenceFile.Writer.keyClass(Text.class),
                SequenceFile.Writer.valueClass(LongWritable.class));

        Text key = new Text();
        try {
            for (long i : regionSplit) {
                key.set(Bytes.toBytes(i));
                writer.append(key, new LongWritable(1l));
            }
        } finally {
            writer.close();
        }


    }

    /**
     * Estimate the cuboid's size
     *
     * @param cuboidId
     * @param rowCount
     * @return the cuboid size in M bytes
     */
    private long estimateCuboidStorageSize(long cuboidId, long rowCount) {

        int bytesLength = RowConstants.ROWKEY_CUBOIDID_LEN;
        Cuboid cuboid = Cuboid.findById(cubeDesc, cuboidId);
        for (TblColRef column : cuboid.getColumns()) {
            bytesLength += colIO.getColumnLength(column);
        }

        // add the measure length
        bytesLength += CubeSizeEstimationCLI.getMeasureSpace(this.cubeDesc);
        return bytesLength * rowCount / (1024L * 1024L);
    }

}
