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

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class FactDistinctColumnPartitioner extends Partitioner<SelfDefineSortableKey, Text> implements Configurable {
    private static final Logger logger = LoggerFactory.getLogger(FactDistinctColumnPartitioner.class);

    private Configuration conf;
    private FactDistinctColumnsReducerMapping reducerMapping;

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;

        KylinConfig config;
        try {
            config = AbstractHadoopJob.loadKylinPropsAndMetadata();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        String cubeName = conf.get(BatchConstants.CFG_CUBE_NAME);
        CubeInstance cube = CubeManager.getInstance(config).getCube(cubeName);

        reducerMapping = new FactDistinctColumnsReducerMapping(cube);
    }

    @Override
    public int getPartition(SelfDefineSortableKey skey, Text value, int numReduceTasks) {
        Text key = skey.getText();
        if (key.getBytes()[0] == FactDistinctColumnsReducerMapping.MARK_FOR_HLL_COUNTER) {
            Long cuboidId = Bytes.toLong(key.getBytes(), 1, Bytes.SIZEOF_LONG);
            return reducerMapping.getReducerIdForCuboidRowCount(cuboidId);
        } else {
            return BytesUtil.readUnsigned(key.getBytes(), 0, 1);
        }
    }

    @Override
    public Configuration getConf() {
        return conf;
    }
}
