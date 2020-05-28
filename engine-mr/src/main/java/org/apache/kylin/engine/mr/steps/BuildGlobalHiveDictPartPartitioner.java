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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;

public class BuildGlobalHiveDictPartPartitioner extends Partitioner<Text, NullWritable> implements Configurable {
    private Configuration conf;

    private Integer[] reduceNumArr;

    @Override
    public void setConf(Configuration configuration) {
        this.conf = configuration;

        KylinConfig config;
        try {
            config = AbstractHadoopJob.loadKylinPropsAndMetadata();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        reduceNumArr = config.getMrHiveDictColumnsReduceNumExcludeRefCols();
    }

    @Override
    public int getPartition(Text key, NullWritable value, int numReduceTasks) {
        // get first byte, the first byte value is the dic col index ,start from 0
        int colIndex = key.getBytes()[0];
        int colReduceNum = reduceNumArr[colIndex];

        int colReduceNumOffset = 0;
        for (int i = 0; i < colIndex; i++) {
            colReduceNumOffset += reduceNumArr[i];
        }

        // Calculate reduce number , reduce num = (value.hash % colReduceNum) + colReduceNumOffset
        byte[] keyBytes = Bytes.copy(key.getBytes(), 1, key.getLength() - 1);
        int hashCode = new Text(keyBytes).hashCode() & 0x7FFFFFFF;
        return hashCode % colReduceNum + colReduceNumOffset;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }
}
