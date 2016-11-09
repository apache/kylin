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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;
import org.apache.kylin.engine.mr.KylinReducer;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author ysong1
 * 
 */
public class RangeKeyDistributionReducer extends KylinReducer<Text, LongWritable, Text, LongWritable> {

    public static final long ONE_GIGA_BYTES = 1024L * 1024L * 1024L;
    private static final Logger logger = LoggerFactory.getLogger(RangeKeyDistributionReducer.class);

    private LongWritable outputValue = new LongWritable(0);

    private int minRegionCount = 1;
    private int maxRegionCount = 500;
    private float cut = 10.0f;
    private int hfileSizeGB = 1;
    private long bytesRead = 0;
    private List<Text> gbPoints = new ArrayList<Text>();
    private String output = null;

    @Override
    protected void setup(Context context) throws IOException {
        super.bindCurrentConfiguration(context.getConfiguration());

        if (context.getConfiguration().get(BatchConstants.CFG_OUTPUT_PATH) != null) {
            output = context.getConfiguration().get(BatchConstants.CFG_OUTPUT_PATH);
        }

        if (context.getConfiguration().get(BatchConstants.CFG_HFILE_SIZE_GB) != null) {
            hfileSizeGB = Float.valueOf(context.getConfiguration().get(BatchConstants.CFG_HFILE_SIZE_GB)).intValue();
        }

        if (context.getConfiguration().get(BatchConstants.CFG_REGION_SPLIT_SIZE) != null) {
            cut = Float.valueOf(context.getConfiguration().get(BatchConstants.CFG_REGION_SPLIT_SIZE));
        }

        if (context.getConfiguration().get(BatchConstants.CFG_REGION_NUMBER_MIN) != null) {
            minRegionCount = Integer.valueOf(context.getConfiguration().get(BatchConstants.CFG_REGION_NUMBER_MIN));
        }

        if (context.getConfiguration().get(BatchConstants.CFG_REGION_NUMBER_MAX) != null) {
            maxRegionCount = Integer.valueOf(context.getConfiguration().get(BatchConstants.CFG_REGION_NUMBER_MAX));
        }

        logger.info("Chosen cut for htable is " + cut + ", max region count=" + maxRegionCount + ", min region count=" + minRegionCount + ", hfile size=" + hfileSizeGB);

        // add empty key at position 0
        gbPoints.add(new Text());
    }

    @Override
    public void doReduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        for (LongWritable v : values) {
            bytesRead += v.get();
        }

        if (bytesRead >= ONE_GIGA_BYTES) {
            gbPoints.add(new Text(key));
            bytesRead = 0; // reset bytesRead
        }
    }

    @Override
    protected void doCleanup(Context context) throws IOException, InterruptedException {
        int nRegion = Math.round((float) gbPoints.size() / cut);
        nRegion = Math.max(minRegionCount, nRegion);
        nRegion = Math.min(maxRegionCount, nRegion);

        int gbPerRegion = gbPoints.size() / nRegion;
        gbPerRegion = Math.max(1, gbPerRegion);

        if (hfileSizeGB <= 0) {
            hfileSizeGB = gbPerRegion;
        }
        int hfilePerRegion = (int) (gbPerRegion / hfileSizeGB);
        hfilePerRegion = Math.max(1, hfilePerRegion);

        System.out.println(nRegion + " regions");
        System.out.println(gbPerRegion + " GB per region");
        System.out.println(hfilePerRegion + " hfile per region");

        Path hfilePartitionFile = new Path(output + "/part-r-00000_hfile");
        SequenceFile.Writer hfilePartitionWriter = new SequenceFile.Writer(hfilePartitionFile.getFileSystem(context.getConfiguration()), context.getConfiguration(), hfilePartitionFile, ImmutableBytesWritable.class, NullWritable.class);
        int hfileCountInOneRegion = 0;
        for (int i = hfileSizeGB; i < gbPoints.size(); i += hfileSizeGB) {
            hfilePartitionWriter.append(new ImmutableBytesWritable(gbPoints.get(i).getBytes()), NullWritable.get());
            if (++hfileCountInOneRegion >= hfilePerRegion) {
                Text key = gbPoints.get(i);
                outputValue.set(i);
                System.out.println(StringUtils.byteToHexString(key.getBytes()) + "\t" + outputValue.get());
                context.write(key, outputValue);

                hfileCountInOneRegion = 0;
            }
        }
        hfilePartitionWriter.close();
    }
}
