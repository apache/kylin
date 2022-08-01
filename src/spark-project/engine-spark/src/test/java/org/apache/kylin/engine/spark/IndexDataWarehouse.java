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

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.NoSuchElementException;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.ZipFileUtils;
import org.apache.kylin.common.persistence.metadata.MetadataStore;
import org.apache.kylin.common.util.TestUtils;
import org.junit.Test;
import org.springframework.util.ReflectionUtils;

import lombok.AllArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AllArgsConstructor
public class IndexDataWarehouse {

    private KylinConfig kylinConfig;

    private String project;

    private String suffix;

    public void reuseBuildData(File outputFolder) throws IOException {
        FileUtils.deleteQuietly(outputFolder);
        ZipFileUtils.decompressZipFile(outputFolder.getAbsolutePath() + ".zip",
                outputFolder.getParentFile().getAbsolutePath());
        FileUtils.copyDirectory(new File(outputFolder, "hdfs"),
                new File(kylinConfig.getHdfsWorkingDirectory().substring(7)));

        val buildConfig = KylinConfig.createKylinConfig(kylinConfig);
        buildConfig.setMetadataUrl(outputFolder.getAbsolutePath() + "/metadata");
        val buildStore = ResourceStore.getKylinMetaStore(buildConfig);
        val store = ResourceStore.getKylinMetaStore(kylinConfig);
        for (String key : store.listResourcesRecursively("/" + project)) {
            store.deleteResource(key);
        }
        for (String key : buildStore.listResourcesRecursively("/" + project)) {
            val raw = buildStore.getResource(key);
            store.deleteResource(key);
            store.putResourceWithoutCheck(key, raw.getByteSource(), System.currentTimeMillis(), 100);
        }
        FileUtils.deleteQuietly(outputFolder);
        log.info("reuse data succeed for {}", outputFolder);
    }

    boolean reuseBuildData() {
        if (!TestUtils.isSkipBuild()) {
            return false;
        }
        try {
            val method = findTestMethod();
            val inputFolder = new File(kylinConfig.getMetadataUrlPrefix()).getParentFile();
            val outputFolder = getOutputFolder(inputFolder, method);
            reuseBuildData(outputFolder);
        } catch (IOException | NoSuchElementException e) {
            log.warn("reuse data failed", e);
            return false;
        }
        return true;
    }

    void persistBuildData() {
        if (!TestUtils.isPersistBuild()) {
            return;
        }
        try {
            val method = findTestMethod();
            val inputFolder = new File(kylinConfig.getMetadataUrlPrefix()).getParentFile();
            val outputFolder = getOutputFolder(inputFolder, method);

            FileUtils.deleteQuietly(outputFolder);
            FileUtils.deleteQuietly(new File(outputFolder.getAbsolutePath() + ".zip"));

            val resourceStore = ResourceStore.getKylinMetaStore(kylinConfig);
            val outputConfig = KylinConfig.createKylinConfig(kylinConfig);
            outputConfig.setMetadataUrl(outputFolder.getCanonicalPath() + "/metadata");
            MetadataStore.createMetadataStore(outputConfig).dump(resourceStore);
            //            FileUtils.copyDirectory(new File(inputFolder, "data"), new File(outputFolder, "data"));
            FileUtils.copyDirectory(new File(kylinConfig.getHdfsWorkingDirectory().substring(7)),
                    new File(outputFolder, "hdfs"));
            ZipFileUtils.compressZipFile(outputFolder.getAbsolutePath(), outputFolder.getAbsolutePath() + ".zip");
            log.info("build data succeed for {}", outputFolder.getName());
            FileUtils.deleteQuietly(outputFolder);
        } catch (Exception e) {
            log.warn("build data failed", e);
        }
    }

    Method findTestMethod() {
        return Arrays.stream(Thread.currentThread().getStackTrace())
                .filter(ele -> ele.getClassName().startsWith("org.apache.kylin")) //
                .map(ele -> {
                    try {
                        return ReflectionUtils.findMethod(Class.forName(ele.getClassName()), ele.getMethodName());
                    } catch (ClassNotFoundException e) {
                        return null;
                    }
                }) //
                .filter(m -> m != null && (m.isAnnotationPresent(Test.class)
                        || m.isAnnotationPresent(org.junit.jupiter.api.Test.class)))
                .reduce((first, second) -> second) //
                .get();
    }

    File getOutputFolder(File inputFolder, Method method) {
        return new File(inputFolder.getParentFile(),
                method.getDeclaringClass().getCanonicalName() + "." + method.getName() + "." + suffix);
    }
}
