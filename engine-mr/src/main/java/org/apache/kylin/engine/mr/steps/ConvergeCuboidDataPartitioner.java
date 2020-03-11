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

import java.util.Random;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.kylin.cube.common.RowKeySplitter;
import org.apache.kylin.engine.mr.common.BatchConstants;

import org.apache.kylin.shaded.com.google.common.base.Preconditions;

public class ConvergeCuboidDataPartitioner extends Partitioner<Text, Text> implements Configurable {

    private Random rand = new Random();

    private Configuration conf;
    private boolean enableSharding;
    private long baseCuboidID;
    private int numReduceBaseCuboid;

    @Override
    public int getPartition(Text key, Text value, int numReduceTasks) {
        long cuboidID = RowKeySplitter.getCuboidId(key.getBytes(), enableSharding);
        // the first numReduceBaseCuboid are for base cuboid
        if (cuboidID == baseCuboidID) {
            return rand.nextInt(numReduceBaseCuboid);
        } else {
            return numReduceBaseCuboid + rand.nextInt(numReduceTasks - numReduceBaseCuboid);
        }
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        String partiParam = conf.get(BatchConstants.CFG_CONVERGE_CUBOID_PARTITION_PARAM);
        String[] params = partiParam.split(",");
        Preconditions.checkArgument(params.length >= 3);
        this.enableSharding = Boolean.parseBoolean(params[0]);
        this.baseCuboidID = Long.parseLong(params[1]);
        this.numReduceBaseCuboid = Integer.parseInt(params[2]);
    }

    @Override
    public Configuration getConf() {
        return conf;
    }
}
