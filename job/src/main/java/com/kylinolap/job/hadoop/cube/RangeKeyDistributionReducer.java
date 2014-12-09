/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kylinolap.job.hadoop.cube;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.StringUtils;

import com.kylinolap.cube.model.CubeDesc.CubeCapacity;
import com.kylinolap.job.constant.BatchConstants;

/**
 * @author ysong1
 * 
 */
public class RangeKeyDistributionReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    public static final long FIVE_GIGA_BYTES = 5L * 1024L * 1024L * 1024L;
    public static final long TEN_GIGA_BYTES = 10L * 1024L * 1024L * 1024L;
    public static final long TWENTY_GIGA_BYTES = 20L * 1024L * 1024L * 1024L;

    private LongWritable outputValue = new LongWritable(0);

    private long bytesRead = 0;
    private Text lastKey;

    private CubeCapacity cubeCapacity;
    private long cut;

    @Override
    protected void setup(Context context) throws IOException {
        cubeCapacity = CubeCapacity.valueOf(context.getConfiguration().get(BatchConstants.CUBE_CAPACITY));
        switch (cubeCapacity) {
        case SMALL:
            cut = FIVE_GIGA_BYTES;
            break;
        case MEDIUM:
            cut = TEN_GIGA_BYTES;
            break;
        case LARGE:
            cut = TWENTY_GIGA_BYTES;
            break;
        }
    }

    @Override
    public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        lastKey = key;
        long length = 0;
        for (LongWritable v : values) {
            length += v.get();
        }

        bytesRead += length;

        if (bytesRead >= cut) {
            outputValue.set(bytesRead);
            context.write(key, outputValue);
            System.out.println(StringUtils.byteToHexString(key.getBytes()) + "\t" + outputValue.get());
            // reset bytesRead
            bytesRead = 0;
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if (lastKey != null) {
            outputValue.set(bytesRead);
            context.write(lastKey, outputValue);
            System.out.println(StringUtils.byteToHexString(lastKey.getBytes()) + "\t" + outputValue.get());
        }
    }
}
