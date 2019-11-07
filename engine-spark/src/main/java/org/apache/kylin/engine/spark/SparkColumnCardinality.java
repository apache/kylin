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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.kylin.common.util.AbstractApplication;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.measure.hllc.HLLCounter;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class SparkColumnCardinality extends AbstractApplication implements Serializable {
    protected static final Logger logger = LoggerFactory.getLogger(SparkColumnCardinality.class);

    public static final Option OPTION_TABLE_NAME = OptionBuilder.withArgName(BatchConstants.ARG_TABLE_NAME).hasArg()
            .isRequired(true).withDescription("Table Name").create(BatchConstants.ARG_TABLE_NAME);
    public static final Option OPTION_OUTPUT = OptionBuilder.withArgName(BatchConstants.ARG_OUTPUT).hasArg()
            .isRequired(true).withDescription("Output").create(BatchConstants.ARG_OUTPUT);
    public static final Option OPTION_PRJ = OptionBuilder.withArgName(BatchConstants.ARG_PROJECT).hasArg()
            .isRequired(true).withDescription("Project name").create(BatchConstants.ARG_PROJECT);
    public static final Option OPTION_COLUMN_COUNT = OptionBuilder.withArgName(BatchConstants.CFG_OUTPUT_COLUMN).hasArg()
            .isRequired(true).withDescription("column count").create(BatchConstants.CFG_OUTPUT_COLUMN);

    private Options options;

    public SparkColumnCardinality() {
        options = new Options();
        options.addOption(OPTION_TABLE_NAME);
        options.addOption(OPTION_OUTPUT);
        options.addOption(OPTION_PRJ);
        options.addOption(OPTION_COLUMN_COUNT);
    }

    @Override
    protected Options getOptions() {
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        String tableName = optionsHelper.getOptionValue(OPTION_TABLE_NAME);
        String output = optionsHelper.getOptionValue(OPTION_OUTPUT);
        int columnCnt = Integer.valueOf(optionsHelper.getOptionValue(OPTION_COLUMN_COUNT));

        Class[] kryoClassArray = new Class[]{Class.forName("scala.reflect.ClassTag$$anon$1"),
                Class.forName("org.apache.kylin.engine.mr.steps.SelfDefineSortableKey")};

        SparkConf conf = new SparkConf().setAppName("Calculate table:" + tableName);
        //set spark.sql.catalogImplementation=hive, If it is not set, SparkSession can't read hive metadata, and throw "org.apache.spark.sql.AnalysisException"
        conf.set("spark.sql.catalogImplementation", "hive");
        //serialization conf
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "org.apache.kylin.engine.spark.KylinKryoRegistrator");
        conf.set("spark.kryo.registrationRequired", "true").registerKryoClasses(kryoClassArray);

        KylinSparkJobListener jobListener = new KylinSparkJobListener();
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            sc.sc().addSparkListener(jobListener);
            HadoopUtil.deletePath(sc.hadoopConfiguration(), new Path(output));
            // table will be loaded by spark sql, so isSequenceFile set false
            final JavaRDD<String[]> recordRDD = SparkUtil.hiveRecordInputRDD(false, sc, null, tableName);
            JavaPairRDD<Integer, Long> resultRdd = recordRDD.mapPartitionsToPair(new BuildHllCounter())
                    .reduceByKey((x, y) -> {
                        x.merge(y);
                        return x;
                    })
                    .mapToPair(record -> {
                        return new Tuple2<>(record._1, record._2.getCountEstimate());
                    })
                    .sortByKey(true, 1)
                    .cache();

            if (resultRdd.count() == 0) {
                ArrayList<Tuple2<Integer, Long>> list = new ArrayList<>();
                for (int i = 0; i < columnCnt; ++i) {
                    list.add(new Tuple2<>(i, 0L));
                }
                JavaPairRDD<Integer, Long> nullRdd = sc.parallelizePairs(list).repartition(1);
                nullRdd.saveAsNewAPIHadoopFile(output, IntWritable.class, LongWritable.class, TextOutputFormat.class);
            } else {
                resultRdd.saveAsNewAPIHadoopFile(output, IntWritable.class, LongWritable.class, TextOutputFormat.class);
            }
        }
    }

    static class BuildHllCounter implements
            PairFlatMapFunction<Iterator<String[]>, Integer, HLLCounter> {

        public BuildHllCounter() {
            logger.info("BuildHllCounter init here.");
        }

        @Override
        public Iterator<Tuple2<Integer, HLLCounter>> call(Iterator<String[]> iterator) throws Exception {
            HashMap<Integer, HLLCounter> hllmap = new HashMap<>();
            while (iterator.hasNext()) {
                String[] values = iterator.next();
                for (int m = 0; m < values.length; ++m) {
                    String fieldValue = values[m];
                    if (fieldValue == null) {
                        fieldValue = "NULL";
                    }
                    getHllc(hllmap, m).add(Bytes.toBytes(fieldValue));
                }
            }
            // convert from hashmap to tuple2(scala).
            List<Tuple2<Integer, HLLCounter>> result = new ArrayList<>();
            for (Map.Entry<Integer, HLLCounter> entry : hllmap.entrySet()) {
                result.add(new Tuple2<>(entry.getKey(), entry.getValue()));
            }
            return result.iterator();
        }

        private HLLCounter getHllc(HashMap<Integer, HLLCounter> hllcMap, Integer key) {
            if (!hllcMap.containsKey(key)) {
                hllcMap.put(key, new HLLCounter());
            }
            return hllcMap.get(key);
        }
    }
}
