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

///*
// * Copyright 2013-2014 eBay Software Foundation
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *   http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package org.apache.kylin.index.cube;
//
//import java.io.IOException;
//
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.util.StringUtils;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
///**
// * @author xjiang
// *
// */
//public class KeyDistributionReducer extends KylinReducer<Text, LongWritable, Text, LongWritable> {
//
//    private static final Logger logger = LoggerFactory.getLogger(KeyDistributionReducer.class);
//
//    private LongWritable outputValue;
//    private boolean isTotalCount;
//    private long totalCount;
//    private int splitNumber;
//    private long splitQuota;
//    private long splitRemain;
//
//    @Override
//    protected void setup(Context context) throws IOException, InterruptedException {
//        super.publishConfiguration(context.getConfiguration());

//        String splitStr = context.getConfiguration().get(KeyDistributionJob.KEY_SPLIT_NUMBER);
//        splitNumber = Integer.valueOf(splitStr).intValue();
//        outputValue = new LongWritable();
//        isTotalCount = true;
//        totalCount = 0;
//        splitQuota = 0;
//        splitRemain = 0;
//    }
//
//    @Override
//    protected void cleanup(Context context) throws IOException, InterruptedException {
//        logger.info("---------------");
//        long splitCount = splitQuota - splitRemain;
//        logger.info("Total Count = " + totalCount + ", Left Count = " + splitCount);
//    }
//
//    @Override
//    public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException,
//            InterruptedException {
//
//        // calculate split quota
//        if (isTotalCount) {
//            for (LongWritable count : values) {
//                totalCount += count.get();
//            }
//            splitQuota = totalCount / splitNumber;
//            splitRemain = splitQuota;
//            isTotalCount = false;
//            return;
//        }
//
//        // output key when split quota is used up 
//        for (LongWritable count : values) {
//            splitRemain -= count.get();
//        }
//        if (splitRemain <= 0) {
//            long splitCount = splitQuota - splitRemain;
//            String hexKey = StringUtils.byteToHexString(key.getBytes());
//            logger.info(hexKey + "\t\t" + splitCount);
//
//            outputValue.set(splitCount);
//            context.write(key, outputValue);
//            splitRemain = splitQuota;
//        }
//
//    }
// }
