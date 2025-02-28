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

package org.apache.kylin.common;

import static org.apache.kylin.common.KylinConfigBase.WRITING_CLUSTER_WORKING_DIR;
import static org.apache.kylin.common.util.TestUtils.getTestConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@MetadataInfo
public class KylinConfigMultithreadingTest {

    @Test
    public void test9PropertiesHotLoadWithMultithreading() throws InterruptedException, ExecutionException {
        final Callable<String> callable1 = () -> {
            reloadFromSiteProperties();
            return "ok";
        };
        final Callable<String> callable2 = () -> {
            readKylinConfig();
            return "ok";
        };
        concurrentTest(10, 30, Lists.newArrayList(callable1, callable2));
    }

    void reloadFromSiteProperties() {
        getTestConfig().setProperty("server.port", "4444");
        //        KylinConfig.getInstanceFromEnv().reloadFromSiteProperties();
        KylinConfig.getInstanceFromEnv().reloadKylinConfigPropertiesFromSiteProperties();
    }

    void readKylinConfig() {
        for (int i = 0; i < 100; i++) {
            KylinConfig config = KylinConfig.getInstanceFromEnv();
            config.getServerPort();
        }
    }

    /**
     * multithreadingToExecuteATaskConcurrently
     *
     * @param concurrentThreads    The number of concurrent threads, which can be used to simulate the number of concurrent users
     * @param times                howManyTimesAreExecutedInTotal
     * @param tasks                 tasks
     */
    public static <T> void concurrentTest(long concurrentThreads, int times, List<Callable<T>> tasks)
            throws InterruptedException, ExecutionException {

        ExecutorService executor = Executors.newFixedThreadPool((int) concurrentThreads);
        List<Future<T>> results = new ArrayList<>(times);

        for (int i = 0; i < times; i++) {
            tasks.forEach(task -> results.add(executor.submit(task)));
        }
        executor.shutdown();

        // The thread pool must be closed at this time, processing task results
        for (Future<T> r : results) {
            Assertions.assertEquals("ok", String.valueOf(r.get()));
        }
    }

    @Test
    public void test8ReloadKylinConfigPropertiesFromSiteProperties() {
        KylinConfig.getInstanceFromEnv().reloadKylinConfigPropertiesFromSiteProperties();
        final Properties oldProperties = KylinConfig.getInstanceFromEnv().exportToProperties();
        KylinConfig.getInstanceFromEnv().reloadKylinConfigPropertiesFromSiteProperties();
        final Properties newProperties = KylinConfig.getInstanceFromEnv().exportToProperties();
        comparePropertiesKeys(oldProperties, newProperties);
    }

    void comparePropertiesKeys(Properties expected, Properties actual) {
        Assertions.assertEquals(expected.size(), actual.size());
        final Set<String> expectedKeys = expected.keySet().stream().map(String::valueOf).collect(Collectors.toSet());
        final Set<String> actualKeys = actual.keySet().stream().map(String::valueOf).collect(Collectors.toSet());
        Assertions.assertTrue(expectedKeys.containsAll(actualKeys));
        Assertions.assertTrue(actualKeys.containsAll(expectedKeys));
    }

    @Test
    public void test7ReloadKylinConfig2Properties() {
        final Properties properties = KylinConfig.getInstanceFromEnv().exportToProperties();
        KylinConfig.getInstanceFromEnv().reloadKylinConfig2Properties(properties);
        final Properties actual = KylinConfig.getInstanceFromEnv().exportToProperties();
        comparePropertiesKeys(properties, actual);
        comparePropertiesValues(properties, actual);
    }

    @Test
    void test8ReloadKylinConfig2Properties() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        final Properties properties = config.exportToProperties();
        properties.setProperty(WRITING_CLUSTER_WORKING_DIR, "file://");
        final Properties actual = config.reloadKylinConfig2Properties(properties);
        Assertions.assertEquals(properties.size(), actual.size());
        // reset
        config.setProperty(WRITING_CLUSTER_WORKING_DIR, "");
    }

    void comparePropertiesValues(Properties expected, Properties actual) {
        Assertions.assertEquals(expected.size(), actual.size());
        final Set<String> expectedKeys = expected.keySet().stream().map(String::valueOf).collect(Collectors.toSet());
        for (String key : expectedKeys) {
            Assertions.assertEquals(expected.get(key), actual.get(key));
        }
    }

    @Test
    public void test5GetMetadataUrlPrefixFromProperties() {
        final KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        final Properties properties = kylinConfig.exportToProperties();
        final String metadataUrlPrefixFromProperties = kylinConfig.getMetadataUrlPrefixFromProperties(properties);
        final String metadataUrlPrefix = kylinConfig.getMetadataUrlPrefix();
        Assertions.assertEquals(metadataUrlPrefix, metadataUrlPrefixFromProperties);
    }

    @Test
    public void test4GetMetadataUrlFromProperties() {
        final KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        final Properties properties = kylinConfig.exportToProperties();
        final StorageURL metadataUrlFromProperties = kylinConfig.getMetadataUrlFromProperties(properties);
        final StorageURL metadataUrl = kylinConfig.getMetadataUrl();

        Assertions.assertEquals(metadataUrl.getScheme(), metadataUrlFromProperties.getScheme());
        Assertions.assertEquals(metadataUrl.getIdentifier(), metadataUrlFromProperties.getIdentifier());
        Assertions.assertEquals(metadataUrl.toString(), metadataUrlFromProperties.toString());
        final Map<String, String> allParameters = metadataUrl.getAllParameters();
        final Map<String, String> allParametersFromProperties = metadataUrlFromProperties.getAllParameters();
        compareMapKeys(allParameters, allParametersFromProperties);
        compareMapValues(allParameters, allParametersFromProperties);
    }

    void compareMapKeys(Map<String, String> expected, Map<String, String> actual) {
        Assertions.assertEquals(expected.size(), actual.size());
        final Set<String> expectedKeys = expected.keySet().stream().map(String::valueOf).collect(Collectors.toSet());
        final Set<String> actualKeys = actual.keySet().stream().map(String::valueOf).collect(Collectors.toSet());
        Assertions.assertTrue(expectedKeys.containsAll(actualKeys));
        Assertions.assertTrue(actualKeys.containsAll(expectedKeys));
    }

    void compareMapValues(Map<String, String> expected, Map<String, String> actual) {
        Assertions.assertEquals(expected.size(), actual.size());
        final Set<String> expectedKeys = expected.keySet().stream().map(String::valueOf).collect(Collectors.toSet());
        for (String key : expectedKeys) {
            Assertions.assertEquals(expected.get(key), actual.get(key));
        }
    }

    @Test
    public void test3getMetadataUrlUniqueIdFromProperties() {
        final KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        final Properties properties = kylinConfig.exportToProperties();
        final String metadataUrlUniqueIdFromProperties = kylinConfig.getMetadataUrlUniqueIdFromProperties(properties);
        final String metadataUrlUniqueId = kylinConfig.getMetadataUrlUniqueId();
        Assertions.assertEquals(metadataUrlUniqueId, metadataUrlUniqueIdFromProperties);
    }

    @Test
    public void test2GetChannelFromProperties() {
        final KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        final Properties properties = kylinConfig.exportToProperties();
        final String channelFromProperties = kylinConfig.getChannelFromProperties(properties);
        final String channel = kylinConfig.getChannel();
        Assertions.assertEquals(channel, channelFromProperties);
    }

    @Test
    public void test1GetOptionalFromProperties() {
        final KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        final Properties properties = kylinConfig.exportToProperties();
        final String portFromProperties = kylinConfig.getOptionalFromProperties("server.port", "7071", properties);
        final String port = kylinConfig.getOptional("server.port", "7071");
        Assertions.assertEquals(port, portFromProperties);
        Assertions.assertEquals("7070", portFromProperties);
    }

    @Test
    public void test6GetHdfsWorkingDirectoryFromProperties() {
        final KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        final Properties properties = kylinConfig.exportToProperties();
        final String hdfsWorkingDirectoryFromProperties = kylinConfig.getHdfsWorkingDirectoryFromProperties(properties);
        final String hdfsWorkingDirectory = kylinConfig.getHdfsWorkingDirectory();
        Assertions.assertEquals(hdfsWorkingDirectory, hdfsWorkingDirectoryFromProperties);
    }
}
