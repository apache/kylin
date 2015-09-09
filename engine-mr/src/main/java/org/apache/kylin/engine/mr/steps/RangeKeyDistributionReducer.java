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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
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
    private int cut = 10;
    private long bytesRead = 0;
    private List<Text> gbPoints = new ArrayList<Text>();

    @Override
    protected void setup(Context context) throws IOException {
        super.bindCurrentConfiguration(context.getConfiguration());

        if (context.getConfiguration().get(BatchConstants.REGION_SPLIT_SIZE) != null) {
            cut = Integer.valueOf(context.getConfiguration().get(BatchConstants.REGION_SPLIT_SIZE));
        }

        if (context.getConfiguration().get(BatchConstants.REGION_NUMBER_MIN) != null) {
            minRegionCount = Integer.valueOf(context.getConfiguration().get(BatchConstants.REGION_NUMBER_MIN));
        }

        if (context.getConfiguration().get(BatchConstants.REGION_NUMBER_MAX) != null) {
            maxRegionCount = Integer.valueOf(context.getConfiguration().get(BatchConstants.REGION_NUMBER_MAX));
        }

        logger.info("Chosen cut for htable is " + cut + ", max region count=" + maxRegionCount + ", min region count =" + minRegionCount);
    }

    @Override
    public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        for (LongWritable v : values) {
            bytesRead += v.get();
        }

        if (bytesRead >= ONE_GIGA_BYTES) {
            gbPoints.add(new Text(key));
            bytesRead = 0; // reset bytesRead
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        int nRegion = Math.round((float) gbPoints.size() / (float) cut);
        nRegion = Math.max(minRegionCount, nRegion);
        nRegion = Math.min(maxRegionCount, nRegion);

        int gbPerRegion = gbPoints.size() / nRegion;
        gbPerRegion = Math.max(1, gbPerRegion);

        System.out.println(nRegion + " regions");
        System.out.println(gbPerRegion + " GB per region");

        for (int i = gbPerRegion; i < gbPoints.size(); i += gbPerRegion) {
            Text key = gbPoints.get(i);
            outputValue.set(i);
            System.out.println(StringUtils.byteToHexString(key.getBytes()) + "\t" + outputValue.get());
            context.write(key, outputValue);
        }
    }
}
