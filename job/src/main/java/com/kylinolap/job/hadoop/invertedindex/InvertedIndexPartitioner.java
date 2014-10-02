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

package com.kylinolap.job.hadoop.invertedindex;

import com.kylinolap.job.constant.BatchConstants;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author yangli9
 */
public class InvertedIndexPartitioner extends Partitioner<LongWritable, ImmutableBytesWritable> implements
        Configurable {

    Configuration conf;
    long timestampGranularity;

    @Override
    public int getPartition(LongWritable key, ImmutableBytesWritable value, int numPartitions) {
        long ts = key.get();
        return (int) (ts / timestampGranularity) % numPartitions;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        this.timestampGranularity = Long.parseLong(conf.get(BatchConstants.TIMESTAMP_GRANULARITY));
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

}
