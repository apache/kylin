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
package io.kyligence.kap.newten.clickhouse;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NDataflowUpdate;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.spark.SparkContext;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.sparkproject.guava.collect.Sets;
import org.testcontainers.containers.JdbcDatabaseContainer;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.transfer.MultipleFileUpload;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import lombok.extern.slf4j.Slf4j;

@Ignore("disable this suite, it is slow")
@Slf4j
@RunWith(JUnit4.class)
public class ClickHouseSimpleITWithS3Test extends ClickHouseSimpleITTest {
    public static final String ACCESS_KEY = "test";
    public static final String SECRET_KEY = "test1234";

    private S3Container s3Container;

    @Override
    protected boolean needHttpServer() {
        return false;
    }

    @Override
    protected void doSetup() throws Exception {
        System.setProperty("kylin.second-storage.wait-index-build-second", "1");
        System.setProperty("kylin.job.scheduler.poll-interval-second", "1");
        s3Container = new S3Container(new S3Container.CredentialsProvider(ACCESS_KEY, SECRET_KEY));
        s3Container.start();
        String endpoint = "http://127.0.0.1:9000";
        String bucket = "test";
        String workDir = "s3a://" + bucket + "/kylin";

        AmazonS3 client = AmazonS3Client.builder()
                .withCredentials(new StaticCredentialsProvider(new BasicAWSCredentials(ACCESS_KEY, SECRET_KEY)))
                .withEndpointConfiguration(
                        new AwsClientBuilder.EndpointConfiguration(endpoint, Regions.US_WEST_2.getName()))
                .build();

        if (!client.doesBucketExist(bucket)) {
            client.createBucket(bucket);
        }

        Map<String, String> overrideConf = Maps.newHashMap();
        // resolve dns problem
        overrideConf.put("fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A");
        overrideConf.put("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        overrideConf.put("fs.s3a.endpoint", endpoint);
        overrideConf.put("fs.s3a.access.key", ACCESS_KEY);
        overrideConf.put("fs.s3a.secret.key", SECRET_KEY);
        overrideConf.put("fs.s3a.connection.ssl.enabled", "false");

        //resolve java.lang.NumberFormatException: For input string: “100M”
        overrideConf.put("fs.s3a.multipart.size", "104857600");

        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kylin.env.hdfs-working-dir", workDir);
        SparkContext sc = ss.sparkContext();
        overrideConf.forEach(sc.hadoopConfiguration()::set);
        //        copyMetaData(manager, bucket, origin, path);
    }

    private void copyMetaData(TransferManager manager, String bucket, String orginPath, String destPath) {
        MultipleFileUpload xfer = manager.uploadDirectory(bucket, destPath.replace("s3a://" + bucket + "/", ""),
                new File(orginPath), true);
        try {
            xfer.waitForCompletion();
        } catch (InterruptedException e) {
            ExceptionUtils.rethrow(e);
        }
    }

    @After
    public void tearDown() throws Exception {
        if (s3Container != null) {
            s3Container.stop();
            s3Container = null;
        }
        super.tearDown();
    }

    @Test
    public void testSingleShard() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse = ClickHouseUtils.startClickHouse()) {
            build_load_query("testSingleShardS3", false, clickhouse);
        }
    }

    @Test
    public void testTwoShards() throws Exception {
        // TODO: make sure splitting data into two shards
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse();
                JdbcDatabaseContainer<?> clickhouse2 = ClickHouseUtils.startClickHouse()) {
            build_load_query("testTwoShardsS3", false, clickhouse1, clickhouse2);
        }
    }

    @Test
    public void testIncrementalSingleShard() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse = ClickHouseUtils.startClickHouse()) {
            build_load_query("testIncrementalSingleShardS3", true, clickhouse);
        }
    }

    @Test
    public void testIncrementalTwoShard() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse();
                JdbcDatabaseContainer<?> clickhouse2 = ClickHouseUtils.startClickHouse()) {
            build_load_query("testIncrementalTwoShardS3", true, clickhouse1, clickhouse2);
        }
    }

    @Override
    protected String getSourceUrl() {
        return "host.docker.internal:" + S3Container.DEFAULT_PORT + "&" + ACCESS_KEY + "&" + SECRET_KEY;
    }

    protected void fullBuild(String dfName) throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, getProject());
        //        Assert.assertTrue(config.getHdfsWorkingDirectory().startsWith("s3"));
        // ready dataflow, segment, cuboid layout
        NDataflow df = dsMgr.getDataflow(dfName);
        // cleanup all segments first
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dsMgr.updateDataflow(update);
        df = dsMgr.getDataflow(dfName);
        List<LayoutEntity> layouts = df.getIndexPlan().getAllLayouts();
        List<LayoutEntity> round1 = Lists.newArrayList(layouts);
        indexDataConstructor.buildIndex(dfName, SegmentRange.TimePartitionedSegmentRange.createInfinite(),
                Sets.newLinkedHashSet(round1), true);
    }

    @Override
    protected void checkHttpServer() throws IOException {
    }
}
