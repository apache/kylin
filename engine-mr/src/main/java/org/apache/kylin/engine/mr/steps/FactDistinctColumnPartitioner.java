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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.kylin.common.util.BytesUtil;

/**
 */
public class FactDistinctColumnPartitioner extends Partitioner<SelfDefineSortableKey, Text> {

    @Override
    public int getPartition(SelfDefineSortableKey skey, Text value, int numReduceTasks) {
        Text key = skey.getText();
        if (key.getBytes()[0] == FactDistinctColumnsMapper.MARK_FOR_HLL) {
            // the last reducer is for merging hll
            return numReduceTasks - 1;
        } else if (key.getBytes()[0] == FactDistinctColumnsMapper.MARK_FOR_PARTITION_COL) {
            // the last but one reducer is for partition col
            return numReduceTasks - 2;
        } else {
            return BytesUtil.readUnsigned(key.getBytes(), 0, 1);
        }
    }
}
