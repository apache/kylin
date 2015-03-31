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

package org.apache.kylin.job.hadoop.invertedindex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import org.apache.kylin.common.mr.KylinReducer;
import org.apache.kylin.job.constant.BatchConstants;

/**
 * @author ysong1
 * 
 */
public class RandomKeyDistributionReducer<KEY extends Writable> extends KylinReducer<KEY, NullWritable, KEY, NullWritable> {


    private Configuration conf;
    private int regionNumber;
    private List<KEY> allSplits;

    @Override
    protected void setup(Context context) throws IOException {
        super.bindCurrentConfiguration(context.getConfiguration());

        conf = context.getConfiguration();
        allSplits = new ArrayList<KEY>();
        regionNumber = Integer.parseInt(context.getConfiguration().get(BatchConstants.REGION_NUMBER));
    }

    @SuppressWarnings("unchecked")
    @Override
    public void reduce(KEY key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        KEY keyCopy = (KEY) ReflectionUtils.newInstance(key.getClass(), conf);
        ReflectionUtils.copy(conf, key, keyCopy);
        allSplits.add(keyCopy);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        int stepLength = allSplits.size() / regionNumber;
        for (int i = stepLength; i < allSplits.size(); i += stepLength) {
            context.write(allSplits.get(i), NullWritable.get());
        }
    }
}
