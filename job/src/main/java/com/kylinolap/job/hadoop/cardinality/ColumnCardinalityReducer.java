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

package com.kylinolap.job.hadoop.cardinality;

import com.kylinolap.common.hll.HyperLogLogPlusCounter;
import com.kylinolap.cube.kv.RowConstants;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * @author Jack
 */
public class ColumnCardinalityReducer extends Reducer<IntWritable, BytesWritable, IntWritable, LongWritable> {

    public static final int ONE = 1;
    private Map<Integer, HyperLogLogPlusCounter> hllcMap = new HashMap<Integer, HyperLogLogPlusCounter>();

    @Override
    public void reduce(IntWritable key, Iterable<BytesWritable> values, Context context) throws IOException,
            InterruptedException {
        for (BytesWritable v : values) {
            int skey = key.get();
            ByteBuffer buffer = ByteBuffer.wrap(v.getBytes());
            HyperLogLogPlusCounter hll = new HyperLogLogPlusCounter();
            hll.readRegisters(buffer);
            getHllc(skey).merge(hll);
            hll.clear();
        }
    }

    private HyperLogLogPlusCounter getHllc(Integer key) {
        if (!hllcMap.containsKey(key)) {
            hllcMap.put(key, new HyperLogLogPlusCounter());
        }
        return hllcMap.get(key);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        List<Integer> keys = new ArrayList<Integer>();
        Iterator<Integer> it = hllcMap.keySet().iterator();
        while (it.hasNext()) {
            keys.add(it.next());
        }
        Collections.sort(keys);
        it = keys.iterator();
        while (it.hasNext()) {
            int key = it.next();
            HyperLogLogPlusCounter hllc = hllcMap.get(key);
            ByteBuffer buf = ByteBuffer.allocate(RowConstants.ROWVALUE_BUFFER_SIZE);
            buf.clear();
            hllc.writeRegisters(buf);
            buf.flip();
            context.write(new IntWritable(key), new LongWritable(hllc.getCountEstimate()));
            //			context.write(new Text("ErrorRate_" + key), new LongWritable((long)hllc.getErrorRate()));
        }

    }
}
