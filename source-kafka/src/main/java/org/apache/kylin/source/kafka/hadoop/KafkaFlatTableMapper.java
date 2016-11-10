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

package org.apache.kylin.source.kafka.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.engine.mr.KylinMapper;

public class KafkaFlatTableMapper extends KylinMapper<LongWritable, BytesWritable, Text, Text> {

    private Text outKey = new Text();
    private Text outValue = new Text();

    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        bindCurrentConfiguration(conf);
    }

    @Override
    public void doMap(LongWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
        outKey.set(Bytes.toBytes(key.get()));
        outValue.set(value.getBytes(), 0, value.getLength());
        context.write(outKey, outValue);
    }
}
