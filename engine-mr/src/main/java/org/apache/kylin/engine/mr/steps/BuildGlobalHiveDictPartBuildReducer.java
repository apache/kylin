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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.engine.mr.KylinReducer;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BuildGlobalHiveDictPartBuildReducer extends KylinReducer<Text, LongWritable, LongWritable, Text> {

    private static final Logger logger = LoggerFactory.getLogger(BuildGlobalHiveDictPartBuildReducer.class);

    private Long count = 0L;
    private MultipleOutputs mos;
    private String[] dicCols;
    private String colName;
    private int colIndex;

    @Override
    protected void doSetup(Context context) throws IOException, InterruptedException {
        mos = new MultipleOutputs(context);
        KylinConfig config;
        try {
            config = AbstractHadoopJob.loadKylinPropsAndMetadata();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        dicCols = config.getMrHiveDictColumnsExcludeRefColumns();
    }

    @Override
    public void doReduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {
        count++;
        byte[] keyBytes = Bytes.copy(key.getBytes(), 1, key.getLength() - 1);
        if (count == 1) {
            colIndex = key.getBytes()[0];
            colName = dicCols[colIndex];
        }

        if (count < 10) {
            logger.info("key:{}, temp dict num :{}, colIndex:{}, colName:{}", key.toString(), count, colIndex, colName);
        }

        mos.write(colIndex + "", new LongWritable(count), new Text(keyBytes), "part_sort/" + colIndex);
    }

    @Override
    protected void doCleanup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();

        String partition = conf.get(MRJobConfig.TASK_PARTITION);
        mos.write(colIndex + "", new LongWritable(count), new Text(partition), "reduce_stats/" + colIndex);
        mos.close();
        logger.info("Reduce partition num {} finish, this reduce done item count is {}", partition, count);
    }
}
