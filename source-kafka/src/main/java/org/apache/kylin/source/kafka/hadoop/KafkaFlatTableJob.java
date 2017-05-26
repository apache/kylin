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
import java.util.Collections;
import java.util.Map;

import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.source.kafka.KafkaConfigManager;
import org.apache.kylin.source.kafka.config.KafkaConfig;
import org.apache.kylin.source.kafka.config.KafkaConsumerProperties;
import org.apache.kylin.source.kafka.util.KafkaClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Run a Hadoop Job to process the stream data in kafka;
 * Modified from the kafka-hadoop-loader in https://github.com/amient/kafka-hadoop-loader
 */
public class KafkaFlatTableJob extends AbstractHadoopJob {
    protected static final Logger logger = LoggerFactory.getLogger(KafkaFlatTableJob.class);

    public static final String CONFIG_KAFKA_PARITION_MIN = "kafka.partition.min";
    public static final String CONFIG_KAFKA_PARITION_MAX = "kafka.partition.max";
    public static final String CONFIG_KAFKA_PARITION_START = "kafka.partition.start.";
    public static final String CONFIG_KAFKA_PARITION_END = "kafka.partition.end.";

    public static final String CONFIG_KAFKA_BROKERS = "kafka.brokers";
    public static final String CONFIG_KAFKA_TOPIC = "kafka.topic";
    public static final String CONFIG_KAFKA_TIMEOUT = "kafka.connect.timeout";
    public static final String CONFIG_KAFKA_CONSUMER_GROUP = "kafka.consumer.group";
    public static final String CONFIG_KAFKA_INPUT_FORMAT = "input.format";
    public static final String CONFIG_KAFKA_PARSER_NAME = "kafka.parser.name";

    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();

        try {
            options.addOption(OPTION_JOB_NAME);
            options.addOption(OPTION_CUBE_NAME);
            options.addOption(OPTION_OUTPUT_PATH);
            options.addOption(OPTION_SEGMENT_ID);
            parseOptions(options, args);

            job = Job.getInstance(getConf(), getOptionValue(OPTION_JOB_NAME));
            String cubeName = getOptionValue(OPTION_CUBE_NAME);
            Path output = new Path(getOptionValue(OPTION_OUTPUT_PATH));

            String segmentId = getOptionValue(OPTION_SEGMENT_ID);

            // ----------------------------------------------------------------------------
            // add metadata to distributed cache
            CubeManager cubeMgr = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
            CubeInstance cube = cubeMgr.getCube(cubeName);

            job.getConfiguration().set(BatchConstants.CFG_CUBE_NAME, cubeName);
            job.getConfiguration().set(BatchConstants.CFG_CUBE_SEGMENT_ID, segmentId);
            logger.info("Starting: " + job.getJobName());

            setJobClasspath(job, cube.getConfig());

            KafkaConfigManager kafkaConfigManager = KafkaConfigManager.getInstance(KylinConfig.getInstanceFromEnv());
            KafkaConfig kafkaConfig = kafkaConfigManager.getKafkaConfig(cube.getRootFactTable());
            String brokers = KafkaClient.getKafkaBrokers(kafkaConfig);
            String topic = kafkaConfig.getTopic();

            if (brokers == null || brokers.length() == 0 || topic == null) {
                throw new IllegalArgumentException("Invalid Kafka information, brokers " + brokers + ", topic " + topic);
            }

            JobEngineConfig jobEngineConfig = new JobEngineConfig(KylinConfig.getInstanceFromEnv());
            job.getConfiguration().addResource(new Path(jobEngineConfig.getHadoopJobConfFilePath(null)));
            KafkaConsumerProperties kafkaConsumerProperties = KafkaConsumerProperties.getInstanceFromEnv();
            job.getConfiguration().addResource(new Path(kafkaConsumerProperties.getKafkaConsumerHadoopJobConf()));
            appendKafkaOverrideProperties(KylinConfig.getInstanceFromEnv(), job.getConfiguration());
            job.getConfiguration().set(CONFIG_KAFKA_BROKERS, brokers);
            job.getConfiguration().set(CONFIG_KAFKA_TOPIC, topic);
            job.getConfiguration().set(CONFIG_KAFKA_TIMEOUT, String.valueOf(kafkaConfig.getTimeout()));
            job.getConfiguration().set(CONFIG_KAFKA_INPUT_FORMAT, "json");
            job.getConfiguration().set(CONFIG_KAFKA_PARSER_NAME, kafkaConfig.getParserName());
            job.getConfiguration().set(CONFIG_KAFKA_CONSUMER_GROUP, cubeName); // use cubeName as consumer group name
            setupMapper(cube.getSegmentById(segmentId));
            job.setNumReduceTasks(0);
            FileOutputFormat.setOutputPath(job, output);
            FileOutputFormat.setCompressOutput(job, true);
            org.apache.log4j.Logger.getRootLogger().info("Output hdfs location: " + output);
            org.apache.log4j.Logger.getRootLogger().info("Output hdfs compression: " + true);
            job.getConfiguration().set(BatchConstants.CFG_OUTPUT_PATH, output.toString());

            deletePath(job.getConfiguration(), output);
            return waitForCompletion(job);

        } catch (Exception e) {
            logger.error("error in KafkaFlatTableJob", e);
            printUsage(options);
            throw e;
        } finally {
            if (job != null)
                cleanupTempConfFile(job.getConfiguration());
        }

    }

    private void setupMapper(CubeSegment cubeSeg) throws IOException {
        // set the segment's offset info to job conf
        Map<Integer, Long> offsetStart = cubeSeg.getSourcePartitionOffsetStart();
        Map<Integer, Long> offsetEnd = cubeSeg.getSourcePartitionOffsetEnd();

        Integer minPartition = Collections.min(offsetStart.keySet());
        Integer maxPartition = Collections.max(offsetStart.keySet());
        job.getConfiguration().set(CONFIG_KAFKA_PARITION_MIN, minPartition.toString());
        job.getConfiguration().set(CONFIG_KAFKA_PARITION_MAX, maxPartition.toString());

        for(Integer partition: offsetStart.keySet()) {
            job.getConfiguration().set(CONFIG_KAFKA_PARITION_START + partition, offsetStart.get(partition).toString());
            job.getConfiguration().set(CONFIG_KAFKA_PARITION_END + partition, offsetEnd.get(partition).toString());
        }

        job.setMapperClass(KafkaFlatTableMapper.class);
        job.setInputFormatClass(KafkaInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setNumReduceTasks(0);
    }

    private static void appendKafkaOverrideProperties(final KylinConfig kylinConfig, Configuration conf) {
        final Map<String, String> kafkaConfOverride = kylinConfig.getKafkaConfigOverride();
        if (kafkaConfOverride.isEmpty() == false) {
            for (String key : kafkaConfOverride.keySet()) {
                conf.set(key, kafkaConfOverride.get(key), "kafka");
            }
        }
    }

    public static void main(String[] args) throws Exception {
        KafkaFlatTableJob job = new KafkaFlatTableJob();
        int exitCode = ToolRunner.run(job, args);
        System.exit(exitCode);
    }

}
