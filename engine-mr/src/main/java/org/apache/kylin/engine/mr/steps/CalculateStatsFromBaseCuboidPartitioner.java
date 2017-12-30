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

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class CalculateStatsFromBaseCuboidPartitioner extends Partitioner<Text, Text> implements Configurable {
    private static final Logger logger = LoggerFactory.getLogger(CalculateStatsFromBaseCuboidPartitioner.class);

    private Configuration conf;
    private int hllShardBase = 1;

    @Override
    public int getPartition(Text key, Text value, int numReduceTasks) {
        Long cuboidId = Bytes.toLong(key.getBytes());
        int shard = cuboidId.hashCode() % hllShardBase;
        if (shard < 0) {
            shard += hllShardBase;
        }
        return numReduceTasks - shard - 1;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        hllShardBase = conf.getInt(BatchConstants.CFG_HLL_REDUCER_NUM, 1);
        logger.info("shard base for hll is " + hllShardBase);
    }

    @Override
    public Configuration getConf() {
        return conf;
    }
}
