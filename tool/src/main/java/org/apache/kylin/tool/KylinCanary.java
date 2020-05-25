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

package org.apache.kylin.tool;


import org.apache.commons.cli.Options;
import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSink;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.apache.http.util.EntityUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.AbstractApplication;
import org.apache.kylin.common.util.OptionsHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class KylinCanary extends AbstractApplication {
    private static final Logger logger = LoggerFactory.getLogger(KylinCanary.class);

    private static final int HTTP_CONNECTION_TIMEOUT = 3000;
    private static final int HTTP_READ_TIMEOUT = 30000;

    private MetricsSink sink;
    private KylinConfig config;
    private long interval;
    private String token;
    private CanaryMetrics canaryMetrics;
    private MetricsSystem metricsSystem;

    @Metrics(name = "canary", about = "Canary metrics", context = "kylin")
    public static class CanaryMetrics {
        @Metric
        MutableGaugeLong queryAvailability;

        public CanaryMetrics() {
        }

        public void setQueryAvailability(long value) {
            queryAvailability.set(value);
        }
    }

    public KylinCanary() {
        config = KylinConfig.getInstanceFromEnv();
        token = config.getCanaryToken();

        if (config.isCanaryDaemon()) {
            interval = config.getCanaryInterval();
        } else {
            interval = -1;
        }

        canaryMetrics = new CanaryMetrics();
        String sinkClassName = config.getCanarySinkClass();
        try {
            sink = (MetricsSink) Class.forName(sinkClassName).getConstructor().newInstance();
        } catch (Exception e) {
            logger.warn("new class: " + sinkClassName + " error.", e);
            sink = new StdOutSink();
        }
        sink.init(null);

        metricsSystem = DefaultMetricsSystem.instance();

        metricsSystem.init("kylin-canary");
        metricsSystem.register("kylin-canary", "Kylin Canary", canaryMetrics);
        metricsSystem.register("sink", "Kylin Canary Sink", sink);
    }

    @Override
    protected Options getOptions() {
        return new Options();
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        if (interval > 0) {
            while (true) {
                runOnce();
            }
        } else {
            runOnce();
        }

        metricsSystem.shutdown();
    }

    private void runOnce() throws InterruptedException {
        long startTime = System.currentTimeMillis();

        String[] servers = config.getRestServers();

        // should use zookeeper info instead
        int total = 0;
        int failed = 0;
        for (int i = 0; i < servers.length; i++) {
            total++;
            try {
                checkQuery(servers[i]);
            } catch (IOException e) {
                failed++;
                logger.warn("check failed with error, ", e);
            }
        }

        double avail = 0.0;
        if (total == 0){
            avail = 1.0;
        } else {
            avail = 1.0 - (failed / total);
        }
        canaryMetrics.setQueryAvailability((long)(avail * 100));

        metricsSystem.publishMetricsNow();

        long finishTime = System.currentTimeMillis();
        logger.info("Finish one turn, consume(ms)=" + (finishTime - startTime)
                + ", interval(ms)=" + interval);
        if (finishTime < startTime + interval) {
            Thread.sleep(startTime + interval - finishTime);
        }
    }

    private HttpResponse makeHttpRequest(String url, String postContent) throws IOException {
        HttpParams httpParams = new BasicHttpParams();
        HttpConnectionParams.setConnectionTimeout(httpParams, HTTP_CONNECTION_TIMEOUT);
        HttpConnectionParams.setSoTimeout(httpParams, HTTP_READ_TIMEOUT);
        DefaultHttpClient httpClient = new DefaultHttpClient(httpParams);

        HttpPost httpPost = new HttpPost(url);
        StringEntity requestEntity = new StringEntity(postContent, ContentType.APPLICATION_JSON);
        httpPost.setEntity(requestEntity);
        httpPost.addHeader("Authorization", "Basic " + token);

        HttpResponse response = httpClient.execute(httpPost);

        return response;
    }

    private long checkQuery(String host) throws IOException {
        long startTime = System.currentTimeMillis();
        String url = "http://" + host + "/kylin/api/query";

        HttpResponse response = makeHttpRequest(url,
                "{\"sql\": \"SELECT 1\",\"project\": \"KYLIN_SYSTEM\"}");

        int code = response.getStatusLine().getStatusCode();
        if (code != 200) {
            String msg = "URL:" + url
                    + " return with Http code " + code
                    + " response body: "
                    + EntityUtils.toString(response.getEntity());
            throw new IOException(msg);
        }

        long finishTime = System.currentTimeMillis();
        long queryTiming = finishTime - startTime;
        return queryTiming;
    }

    // Simple implementation of canary sink that allows to plot on
    // file or standard output timings or failures.
    public static class StdOutSink implements MetricsSink {
        @Override
        public void init(SubsetConfiguration subsetConfiguration) {

        }

        @Override
        public void putMetrics(MetricsRecord record) {
            // LOG.info("Metric: " + metricsRecord);
            for (AbstractMetric metric : record.metrics()) {
                logger.info("metric: " + metric.name() + " : " + metric.value());
            }
        }

        @Override
        public void flush() {
        }
    }

    public static void main(String[] args) throws Exception {
        new KylinCanary().execute(args);
    }
}

