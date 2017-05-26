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
package org.apache.kylin.engine.spark;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.kylin.common.util.AbstractApplication;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

/**
 */
public class SparkCountDemo extends AbstractApplication {

    private static final Option OPTION_INPUT_PATH = OptionBuilder.withArgName("path").hasArg().isRequired(true).withDescription("Input path").create("input");

    private Options options;

    public SparkCountDemo() {
        options = new Options();
        //        options.addOption(OPTION_INPUT_PATH);
    }

    @Override
    protected Options getOptions() {
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        String logFile = "hdfs://10.249.65.231:8020/tmp/kylin.properties"; // Should be some file on your system
        SparkConf conf = new SparkConf().setAppName("Simple Application");
        JavaSparkContext sc = new JavaSparkContext(conf);
        final JavaPairRDD<String, Integer> logData = sc.textFile(logFile).mapToPair(new PairFunction<String, String, Integer>() {

            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, s.length());
            }
        }).sortByKey();
        logData.persist(StorageLevel.MEMORY_AND_DISK_SER());

        System.out.println("line number:" + logData.count());

        logData.mapToPair(new PairFunction<Tuple2<String, Integer>, ImmutableBytesWritable, KeyValue>() {
            @Override
            public Tuple2<ImmutableBytesWritable, KeyValue> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                ImmutableBytesWritable key = new ImmutableBytesWritable(stringIntegerTuple2._1().getBytes());
                KeyValue value = new KeyValue(stringIntegerTuple2._1().getBytes(), "f".getBytes(), "c".getBytes(), String.valueOf(stringIntegerTuple2._2()).getBytes());
                return new Tuple2(key, value);
            }
        }).saveAsNewAPIHadoopFile("hdfs://10.249.65.231:8020/tmp/hfile", ImmutableBytesWritable.class, KeyValue.class, HFileOutputFormat.class);

    }
}
