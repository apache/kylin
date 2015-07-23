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
//import org.apache.hadoop.mapreduce.Mapper;
//
///**
// * @author xjiang
// *
// */
//public class KeyDistributionMapper extends KylinMapper<Text, Text, Text, LongWritable> {
//
//    private int headerLength;
//
//    private Text currentKey;
//    private long outputLong;
//    private Text outputKey;
//    private LongWritable outputValue;
//    private int columnPercentage;
//    private int allRowCount;
//
//    @Override
//    protected void setup(Context context) throws IOException {
//super.publishConfiguration(context.getConfiguration());

//        String percentStr = context.getConfiguration().get(KeyDistributionJob.KEY_COLUMN_PERCENTAGE);
//        this.columnPercentage = Integer.valueOf(percentStr).intValue();
//        if (this.columnPercentage <= 0 || this.columnPercentage >= 100) {
//            this.columnPercentage = 20;
//        }
//        String headerLenStr = context.getConfiguration().get(KeyDistributionJob.KEY_HEADER_LENGTH);
//        this.headerLength = Integer.valueOf(headerLenStr).intValue();
//
//        currentKey = new Text();
//        outputLong = 0;
//        outputKey = new Text();
//        outputValue = new LongWritable(1);
//        allRowCount = 0;
//    }
//
//    @Override
//    protected void cleanup(Context context) throws IOException, InterruptedException {
//        emit(context); // emit the last holding record
//
//        byte[] zerokey = new byte[] { 0 };
//        outputKey.set(zerokey);
//        outputValue.set(allRowCount);
//        context.write(outputKey, outputValue);
//    }
//
//    @Override
//    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
//        byte[] bytes = key.getBytes();
//        int columnLength = bytes.length - this.headerLength;
//        int columnPrefixLen = columnLength * this.columnPercentage / 100;
//        if (columnPrefixLen == 0 && columnLength > 0) {
//            columnPrefixLen = 1;
//        }
//        if (columnPrefixLen > 0) {
//            currentKey.set(bytes, 0, this.headerLength + columnPrefixLen);
//        } else {
//            currentKey.set(bytes);
//        }
//
//        allRowCount++;
//
//        if (outputKey.getLength() == 0) { // first record
//            outputKey.set(currentKey);
//            outputLong = 1;
//        } else if (outputKey.equals(currentKey)) { // same key, note input is sorted
//            outputLong++;
//        } else { // the next key
//            emit(context);
//            outputKey.set(currentKey);
//            outputLong = 1;
//        }
//    }
//
//    private void emit(Context context) throws IOException, InterruptedException {
//        if (outputLong == 0)
//            return;
//
//        outputValue.set(outputLong);
//        context.write(outputKey, outputValue);
//    }
// }
