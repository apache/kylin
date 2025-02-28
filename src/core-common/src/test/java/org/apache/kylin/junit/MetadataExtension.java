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
package org.apache.kylin.junit;

import java.io.File;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Field;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.Singletons;
import org.apache.kylin.common.SystemPropertiesCache;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.TempMetadataBuilder;
import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.InvocationInterceptor;
import org.junit.platform.commons.support.AnnotationSupport;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;

public class MetadataExtension implements BeforeEachCallback, BeforeAllCallback, InvocationInterceptor {

    private static final ExtensionContext.Namespace NAMESPACE = ExtensionContext.Namespace
            .create(MetadataExtension.class);
    private static final String METADATA_INFO_KEY = "MetadataInfo";

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        readFromAnnotation(context.getElement()).ifPresent(x -> context.getStore(NAMESPACE).put(METADATA_INFO_KEY, x));
    }

    @SneakyThrows
    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        run(context, null);
    }

    private Optional<MetadataResource> readFromAnnotation(Optional<AnnotatedElement> element) {
        return AnnotationSupport.findAnnotation(element, MetadataInfo.class).map(MetadataResource::of);
    }

    private void run(ExtensionContext context, Invocation<Void> invocation) throws Throwable {
        readFromAnnotation(context.getElement())
                .orElse(context.getStore(NAMESPACE).get(METADATA_INFO_KEY, MetadataResource.class)).get();
        if (invocation != null) {
            invocation.proceed();
        }
    }

    @RequiredArgsConstructor
    private static class MetadataResource implements ExtensionContext.Store.CloseableResource {

        private TempMetadataBuilder metadataBuilder;
        private File tempMetadataDirectory;

        static MetadataResource of(MetadataInfo info) {
            val resource = new MetadataResource();
            resource.metadataBuilder = TempMetadataBuilder.createBuilder(Lists.newArrayList(info.overlay()));
            resource.metadataBuilder.setProject(info.project());
            resource.metadataBuilder.setOnlyProps(info.onlyProps());
            return resource;

        }

        public File get() {
            String tempMetadataDir = metadataBuilder.build();
            KylinConfig.setKylinConfigForLocalTest(tempMetadataDir);
            tempMetadataDirectory = new File(tempMetadataDir);
            try {
                Class.forName("org.h2.Driver");
            } catch (ClassNotFoundException e) {
                // ignore it
            }
            cleanSingletonInstances();

            val kylinHomePath = new File(getTestConfig().getMetadataUrl().toString()).getParentFile().getAbsolutePath();
            SystemPropertiesCache.setProperty("KYLIN_HOME", kylinHomePath);
            val jobJar = org.apache.kylin.common.util.FileUtils.findFile(
                    new File(kylinHomePath, "../../../assembly/target/").getAbsolutePath(), "kylin-assembly(.?)\\.jar");
            KylinConfig testConfig = getTestConfig();
            testConfig.setProperty("kylin.engine.spark.job-jar", jobJar == null ? "" : jobJar.getAbsolutePath());
            testConfig.setProperty("kylin.query.security.acl-tcr-enabled", "false");
            return tempMetadataDirectory;
        }

        @Override
        public void close() throws Throwable {
            cleanSingletonInstances();
            clearTestConfig();
            SystemPropertiesCache.clearProperty("KYLIN_HOME");
            QueryContext.reset();

            FileUtils.deleteQuietly(tempMetadataDirectory);
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

        static ConcurrentHashMap<Class, ConcurrentHashMap<String, Object>> getInstanceByProjectFromSingleton()
                throws Exception {
            Field instanceField = Singletons.class.getDeclaredField("instance");
            Unsafe.changeAccessibleObject(instanceField, true);
            Field field = Singletons.class.getDeclaredField("instancesByPrj");
            Unsafe.changeAccessibleObject(field, true);
            val result = (ConcurrentHashMap<Class, ConcurrentHashMap<String, Object>>) field
                    .get(instanceField.get(null));
            if (result == null) {
                field.set(instanceField.get(null), Maps.newConcurrentMap());
            }
            return (ConcurrentHashMap<Class, ConcurrentHashMap<String, Object>>) field.get(instanceField.get(null));
        }

        static ConcurrentHashMap<Class, ConcurrentHashMap<String, Object>> getInstanceByProject() throws Exception {
            Field singletonField = getTestConfig().getClass().getDeclaredField("singletons");
            Unsafe.changeAccessibleObject(singletonField, true);
            Field field = Singletons.class.getDeclaredField("instancesByPrj");
            Unsafe.changeAccessibleObject(field, true);
            return (ConcurrentHashMap<Class, ConcurrentHashMap<String, Object>>) field
                    .get(singletonField.get(getTestConfig()));
        }

        static ConcurrentHashMap<Class, Object> getInstancesFromSingleton() throws Exception {
            Field instanceField = Singletons.class.getDeclaredField("instance");
            Unsafe.changeAccessibleObject(instanceField, true);
            Field field = Singletons.class.getDeclaredField("instancesByPrj");
            Unsafe.changeAccessibleObject(field, true);
            val result = (ConcurrentHashMap<Class, ConcurrentHashMap<String, Object>>) field
                    .get(instanceField.get(null));
            if (result == null) {
                field.set(instanceField.get(null), Maps.newConcurrentMap());
            }
            return (ConcurrentHashMap<Class, Object>) field.get(instanceField.get(null));
        }

        static ConcurrentHashMap<Class, Object> getInstances() throws Exception {
            Field singletonField = getTestConfig().getClass().getDeclaredField("singletons");
            Unsafe.changeAccessibleObject(singletonField, true);
            Field filed = Singletons.class.getDeclaredField("instances");
            Unsafe.changeAccessibleObject(filed, true);
            return (ConcurrentHashMap<Class, Object>) filed.get(singletonField.get(getTestConfig()));
        }

        static ConcurrentHashMap<Class, Object> getGlobalInstances() throws Exception {
            Field instanceFiled = Singletons.class.getDeclaredField("instance");
            Unsafe.changeAccessibleObject(instanceFiled, true);

            Singletons instanceSingle = (Singletons) instanceFiled.get(instanceFiled);

            Field instancesField = instanceSingle.getClass().getDeclaredField("instances");
            Unsafe.changeAccessibleObject(instancesField, true);

            return (ConcurrentHashMap<Class, Object>) instancesField.get(instanceSingle);
        }

        static KylinConfig getTestConfig() {
            return KylinConfig.getInstanceFromEnv();
        }

        static void clearTestConfig() {
            try {
                ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv()).close();
            } catch (Exception ignore) {
            }
            KylinConfig.destroyInstance();
        }

    }
}
