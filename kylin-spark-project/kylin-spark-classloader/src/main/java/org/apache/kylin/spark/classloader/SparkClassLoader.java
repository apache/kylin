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

package org.apache.kylin.spark.classloader;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.HashSet;
import java.util.Set;

public class SparkClassLoader extends URLClassLoader {
    //preempt these classes from parent
    private static String[] SPARK_CL_PREEMPT_CLASSES = new String[] {"org.apache.spark", "scala.",
            "org.spark_project", "com.esotericsoftware.kryo"};

    //preempt these files from parent
    private static String[] SPARK_CL_PREEMPT_FILES = new String[] {"spark-version-info.properties", "HiveClientImpl",
            "org/apache/spark"};

    //when loading class (indirectly used by SPARK_CL_PREEMPT_CLASSES), some of them should NOT use parent's first
    private static String[] THIS_CL_PRECEDENT_CLASSES = new String[] {"javax.ws.rs", "org.apache.hadoop.hive"};

    //when loading class (indirectly used by SPARK_CL_PREEMPT_CLASSES), some of them should use parent's first
    private static String[] PARENT_CL_PRECEDENT_CLASSES = new String[] {
            // Java standard library:
            "com.sun.", "launcher.", "java.", "javax.", "org.ietf", "org.omg", "org.w3c", "org.xml", "sunw.", "sun.",
            // logging
            "org.apache.commons.logging", "org.apache.log4j", "org.slf4j", "org.apache.hadoop",
            // Hadoop/ZK:
            "org.apache.kylin", "com.intellij", "org.apache.calcite"};

    private static final Set<String> classNotFoundCache = new HashSet<>();
    private static Logger logger = LoggerFactory.getLogger(SparkClassLoader.class);

    static {
        String sparkClassLoaderSparkClPreemptClasses = System.getenv("SPARKCLASSLOADER_SPARK_CL_PREEMPT_CLASSES");
        if (!StringUtils.isEmpty(sparkClassLoaderSparkClPreemptClasses)) {
            SPARK_CL_PREEMPT_CLASSES = StringUtils.split(sparkClassLoaderSparkClPreemptClasses, ",");
        }

        String sparkClassLoaderSparkClPreemptFiles = System.getenv("SPARKCLASSLOADER_SPARK_CL_PREEMPT_FILES");
        if (!StringUtils.isEmpty(sparkClassLoaderSparkClPreemptFiles)) {
            SPARK_CL_PREEMPT_FILES = StringUtils.split(sparkClassLoaderSparkClPreemptFiles, ",");
        }

        String sparkClassLoaderThisClPrecedentClasses = System.getenv("SPARKCLASSLOADER_THIS_CL_PRECEDENT_CLASSES");
        if (!StringUtils.isEmpty(sparkClassLoaderThisClPrecedentClasses)) {
            THIS_CL_PRECEDENT_CLASSES = StringUtils.split(sparkClassLoaderThisClPrecedentClasses, ",");
        }

        String sparkClassLoaderParentClPrecedentClasses = System
                .getenv("SPARKCLASSLOADER_PARENT_CL_PRECEDENT_CLASSES");
        if (!StringUtils.isEmpty(sparkClassLoaderParentClPrecedentClasses)) {
            PARENT_CL_PRECEDENT_CLASSES = StringUtils.split(sparkClassLoaderParentClPrecedentClasses, ",");
        }

        try {
            final Method registerParallel = ClassLoader.class.getDeclaredMethod("registerAsParallelCapable");
            AccessController.doPrivileged(new PrivilegedAction<Object>() {
                public Object run() {
                    registerParallel.setAccessible(true);
                    return null;
                }
            });
            Boolean result = (Boolean) registerParallel.invoke(null);
            if (!result) {
                logger.warn("registrationFailed");
            }
        } catch (Exception ignore) {

        }
    }

    /**
     * Creates a DynamicClassLoader that can load classes dynamically
     * from jar files under a specific folder.
     *
     * @param parent the parent ClassLoader to set.
     */
    protected SparkClassLoader(ClassLoader parent) throws IOException {
        super(new URL[] {}, parent);
        init();
    }

    public void init() throws MalformedURLException {
        String sparkHome = System.getenv("SPARK_HOME");
        if (sparkHome == null) {
            sparkHome = System.getProperty("SPARK_HOME");
            if (sparkHome == null) {
                throw new RuntimeException(
                        "Spark home not found; set it explicitly or use the SPARK_HOME environment variable.");
            }
        }
        File file = new File(sparkHome + "/jars");
        File[] jars = file.listFiles();
        for (File jar : jars) {
            addURL(jar.toURI().toURL());
        }

    }

    @Override
    public Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {

        if (needToUseGlobal(name)) {
            logger.debug("delegate " + name + " directly to parent");
            return super.loadClass(name, resolve);
        }
        return doLoadclass(name);
    }

    private Class<?> doLoadclass(String name) throws ClassNotFoundException {
        synchronized (getClassLoadingLock(name)) {
            // Check whether the class has already been loaded:
            Class<?> clasz = findLoadedClass(name);
            if (clasz != null) {
                logger.debug("Class " + name + " already loaded");
            } else {
                try {
                    // Try to find this class using the URLs passed to this ClassLoader
                    logger.debug("Finding class: " + name);
                    clasz = super.findClass(name);
                    if (clasz == null) {
                        logger.debug("cannot find class" + name);
                    }
                } catch (ClassNotFoundException e) {
                    classNotFoundCache.add(name);
                    // Class not found using this ClassLoader, so delegate to parent
                    logger.debug("Class " + name + " not found - delegating to parent");
                    try {
                        // sparder and query module has some class start with org.apache.spark,
                        // We need to use some lib that does not exist in spark/jars
                        clasz = getParent().loadClass(name);
                    } catch (ClassNotFoundException e2) {
                        // Class not found in this ClassLoader or in the parent ClassLoader
                        // Log some debug output before re-throwing ClassNotFoundException
                        logger.debug("Class " + name + " not found in parent loader");
                        throw e2;
                    }
                }
            }
            return clasz;
        }
    }

    private boolean isThisCLPrecedent(String name) {
        for (String exemptPrefix : THIS_CL_PRECEDENT_CLASSES) {
            if (name.startsWith(exemptPrefix)) {
                return true;
            }
        }
        return false;
    }

    private boolean isParentCLPrecedent(String name) {
        for (String exemptPrefix : PARENT_CL_PRECEDENT_CLASSES) {
            if (name.startsWith(exemptPrefix)) {
                return true;
            }
        }
        return false;
    }

    private boolean needToUseGlobal(String name) {
        return !isThisCLPrecedent(name) && !classNeedPreempt(name) && isParentCLPrecedent(name);
    }

    boolean classNeedPreempt(String name) {
        if (classNotFoundCache.contains(name)) {
            return false;
        }
        for (String exemptPrefix : SPARK_CL_PREEMPT_CLASSES) {
            if (name.startsWith(exemptPrefix)) {
                return true;
            }
        }
        return false;
    }

    boolean fileNeedPreempt(String name) {
        for (String exemptPrefix : SPARK_CL_PREEMPT_FILES) {
            if (name.contains(exemptPrefix)) {
                return true;
            }
        }
        return false;
    }
}
