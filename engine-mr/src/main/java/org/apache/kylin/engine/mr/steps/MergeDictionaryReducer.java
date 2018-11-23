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

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.engine.mr.KylinReducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MergeDictionaryReducer extends KylinReducer<IntWritable, Text, Text, Text> {

    private static final Logger logger = LoggerFactory.getLogger(MergeDictionaryReducer.class);

    @Override
    protected void doReduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        for (Text text : values) {
            String value = text.toString();
            String[] splited = StringUtil.split(value, "=");
            if (splited != null && splited.length == 2) {
                logger.info("Dictionary for col {}, save at {}", splited[0], splited[1]);
                context.write(new Text(splited[0]), new Text(splited[1]));
            }
        }
    }
}
