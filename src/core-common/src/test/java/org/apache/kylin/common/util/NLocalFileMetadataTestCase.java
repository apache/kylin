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

package org.apache.kylin.common.util;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.AbstractTestCase;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.Singletons;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.persistence.ResourceStore;
import org.junit.Assert;
import org.junit.Before;
import org.mockito.Mockito;

import io.kyligence.kap.guava20.shaded.common.collect.Lists;
import io.kyligence.kap.guava20.shaded.common.collect.Maps;
import lombok.val;

@Deprecated
public class NLocalFileMetadataTestCase extends AbstractTestCase {

    protected static File tempMetadataDirectory = null;
    Map<Object, Object> originManager = Maps.newHashMap();

    @Before
    public void setNeedCheckCC() {
        overwriteSystemProp("needCheckCC", "true");
    }

    public static File getTempMetadataDirectory() {
        return tempMetadataDirectory;
    }

    public static ConcurrentHashMap<Class, ConcurrentHashMap<String, Object>> getInstanceByProjectFromSingleton()
            throws Exception {
        Field instanceField = Singletons.class.getDeclaredField("instance");
        Unsafe.changeAccessibleObject(instanceField, true);
        Field field = Singletons.class.getDeclaredField("instancesByPrj");
        Unsafe.changeAccessibleObject(field, true);
        val result = (ConcurrentHashMap<Class, ConcurrentHashMap<String, Object>>) field.get(instanceField.get(null));
        if (result == null) {
            field.set(instanceField.get(null), Maps.newConcurrentMap());
        }
        return (ConcurrentHashMap<Class, ConcurrentHashMap<String, Object>>) field.get(instanceField.get(null));
    }

    public static ConcurrentHashMap<Class, ConcurrentHashMap<String, Object>> getInstanceByProject() throws Exception {
        Field singletonField = getTestConfig().getClass().getDeclaredField("singletons");
        Unsafe.changeAccessibleObject(singletonField, true);
        Field field = Singletons.class.getDeclaredField("instancesByPrj");
        Unsafe.changeAccessibleObject(field, true);
        return (ConcurrentHashMap<Class, ConcurrentHashMap<String, Object>>) field
                .get(singletonField.get(getTestConfig()));
    }

    public static ConcurrentHashMap<Class, Object> getInstancesFromSingleton() throws Exception {
        Field instanceField = Singletons.class.getDeclaredField("instance");
        Unsafe.changeAccessibleObject(instanceField, true);
        Field field = Singletons.class.getDeclaredField("instancesByPrj");
        Unsafe.changeAccessibleObject(field, true);
        val result = (ConcurrentHashMap<Class, ConcurrentHashMap<String, Object>>) field.get(instanceField.get(null));
        if (result == null) {
            field.set(instanceField.get(null), Maps.newConcurrentMap());
        }
        return (ConcurrentHashMap<Class, Object>) field.get(instanceField.get(null));
    }

    public static ConcurrentHashMap<Class, Object> getInstances() throws Exception {
        Field singletonField = getTestConfig().getClass().getDeclaredField("singletons");
        Unsafe.changeAccessibleObject(singletonField, true);
        Field filed = Singletons.class.getDeclaredField("instances");
        Unsafe.changeAccessibleObject(filed, true);
        return (ConcurrentHashMap<Class, Object>) filed.get(singletonField.get(getTestConfig()));
    }

    public static ConcurrentHashMap<Class, Object> getGlobalInstances() throws Exception {
        Field instanceFiled = Singletons.class.getDeclaredField("instance");
        Unsafe.changeAccessibleObject(instanceFiled, true);

        Singletons instanceSingle = (Singletons) instanceFiled.get(instanceFiled);

        Field instancesField = instanceSingle.getClass().getDeclaredField("instances");
        Unsafe.changeAccessibleObject(instancesField, true);

        return (ConcurrentHashMap<Class, Object>) instancesField.get(instanceSingle);
    }

    public <T> T spyManagerByProject(T t, Class<T> tClass,
            ConcurrentHashMap<Class, ConcurrentHashMap<String, Object>> cache, String project) {
        T manager = Mockito.spy(t);
        originManager.put(manager, t);
        ConcurrentHashMap<Class, ConcurrentHashMap<String, Object>> managersByPrjCache = cache;
        if (managersByPrjCache.get(tClass) == null) {
            managersByPrjCache.put(tClass, new ConcurrentHashMap<>());
        }
        managersByPrjCache.get(tClass).put(project, manager);
        return manager;
    }

    public <T> T spyManagerByProject(T t, Class<T> tClass, String project) throws Exception {
        return spyManagerByProject(t, tClass, getInstanceByProject(), project);
    }

    public <T> T spyManager(T t, Class<T> tClass) throws Exception {
        T manager = Mockito.spy(t);
        originManager.put(manager, t);
        ConcurrentHashMap<Class, Object> managersCache = getInstances();
        managersCache.put(tClass, manager);
        return manager;
    }

    public <T, M> T spy(M m, Function<M, T> functionM, Function<T, T> functionT) {
        return functionM.apply(Mockito.doAnswer(answer -> {
            T t = functionM.apply((M) originManager.get(m));
            return functionT.apply(t);
        }).when(m));
    }

    public void createTestMetadata(String... overlay) {
        staticCreateTestMetadata(overlay);
        val kylinHomePath = new File(getTestConfig().getMetadataUrl().toString()).getParentFile().getAbsolutePath();
        overwriteSystemProp("KYLIN_HOME", kylinHomePath);
        val jobJar = org.apache.kylin.common.util.FileUtils.findFile(
                new File(kylinHomePath, "../../../assembly/target/").getAbsolutePath(), "ke-assembly(.*?)\\.jar");
        getTestConfig().setProperty("kylin.engine.spark.job-jar", jobJar == null ? "" : jobJar.getAbsolutePath());
        getTestConfig().setProperty("kylin.query.security.acl-tcr-enabled", "false");
        getTestConfig().setProperty("kylin.streaming.enabled", "true");
    }

    public void cleanupTestMetadata() {
        QueryContext.reset();
        staticCleanupTestMetadata();
    }

    public static void staticCreateTestMetadata(String... overlay) {
        String tempMetadataDir = TempMetadataBuilder.prepareLocalTempMetadata(Lists.newArrayList(overlay));
        KylinConfig.setKylinConfigForLocalTest(tempMetadataDir);
        tempMetadataDirectory = new File(tempMetadataDir);
        getTestConfig().setProperty("kylin.query.security.acl-tcr-enabled", "false");
        try {
            Class.forName("org.h2.Driver");
        } catch (ClassNotFoundException e) {
            // ignore it
        }
        cleanSingletonInstances();
    }

    private static void cleanSingletonInstances() {
        try {
            getInstances().clear();
        } catch (Exception e) {
            //ignore in it
        }

        try {
            getGlobalInstances().clear();
        } catch (Exception e) {
            //ignore in it
        }

        try {
            getInstancesFromSingleton().clear();
        } catch (Exception e) {
            //ignore in it
        }

        try {
            getInstanceByProjectFromSingleton().clear();
        } catch (Exception e) {
            //ignore in it
        }

        try {
            getInstanceByProject().clear();
        } catch (Exception e) {
            //ignore in it
        }
    }

    private static void clearTestConfig() {
        try {
            ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv()).close();
        } catch (Exception ignore) {
        }
        KylinConfig.destroyInstance();
    }

    public static KylinConfig getTestConfig() {
        return KylinConfig.getInstanceFromEnv();
    }

    public static void staticCleanupTestMetadata() {
        cleanSingletonInstances();
        clearTestConfig();
        QueryContext.reset();

        File directory = new File(TempMetadataBuilder.TEMP_TEST_METADATA);
        FileUtils.deleteQuietly(directory);

    }

    public static String getLocalWorkingDirectory() {
        String dir = KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory();
        if (dir.startsWith("file://"))
            dir = dir.substring("file://".length());
        try {
            return new File(dir).getCanonicalPath();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    protected ResourceStore getStore() {
        return ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
    }

    protected Map<Integer, Long> createKafkaPartitionOffset(int partition, Long offset) {
        Map<Integer, Long> map = new HashMap<Integer, Long>();
        map.put(partition, offset);
        return map;
    }

    protected Map<Integer, Long> createKafkaPartitionsOffset(int partitionNumbers, Long offset) {
        Map<Integer, Long> map = new HashMap<Integer, Long>();
        for (int i = 0; i < partitionNumbers; i++) {
            map.put(i, offset);
        }
        return map;
    }

    public void assertKylinExeption(UserFunction f, String msg) {
        try {
            f.process();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            if (StringUtils.isNotEmpty(msg)) {
                Assert.assertTrue(e.getMessage().contains(msg));
            }
        }
    }

    public void assertRuntimeExeption(UserFunction f, String msg) {
        try {
            f.process();
            Assert.fail();
        } catch (Exception e) {
            if (StringUtils.isNotEmpty(msg)) {
                Assert.assertTrue(e.getMessage().contains(msg));
            }
        }
    }

    public interface UserFunction {
        void process() throws Exception;
    }
}
