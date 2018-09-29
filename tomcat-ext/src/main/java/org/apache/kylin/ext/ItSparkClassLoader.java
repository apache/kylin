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

package org.apache.kylin.ext;

import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Set;

import static org.apache.kylin.ext.ClassLoaderUtils.findFile;

public class ItSparkClassLoader extends URLClassLoader {
    private static final String[] SPARK_CL_PREEMPT_CLASSES = new String[] { "org.apache.spark", "scala.",
            "org.spark_project"
            //            "javax.ws.rs.core.Application",
            //            "javax.ws.rs.core.UriBuilder", "org.glassfish.jersey", "javax.ws.rs.ext"
            //user javax.ws.rs.api 2.01  not jersey-core-1.9.jar
    };
    private static final String[] SPARK_CL_PREEMPT_FILES = new String[] { "spark-version-info.properties",
            "HiveClientImpl", "org/apache/spark" };

    private static final String[] THIS_CL_PRECEDENT_CLASSES = new String[] { "javax.ws.rs", "org.apache.hadoop.hive" };

    private static final String[] PARENT_CL_PRECEDENT_CLASSES = new String[] {
            //            // Java standard library:
            "com.sun.", "launcher.", "java.", "javax.", "org.ietf", "org.omg", "org.w3c", "org.xml", "sunw.", "sun.",
            // logging
            "org.apache.commons.logging", "org.apache.log4j", "com.hadoop", "org.slf4j",
            // Hadoop/HBase/ZK:
            "org.apache.hadoop", "org.apache.zookeeper", "org.apache.kylin", "com.intellij",
            "org.apache.calcite", "org.roaringbitmap", "org.apache.parquet" };
    private static final Set<String> classNotFoundCache = Sets.newHashSet();
    private static Logger logger = LoggerFactory.getLogger(ItSparkClassLoader.class);

    /**
     * Creates a DynamicClassLoader that can load classes dynamically
     * from jar files under a specific folder.
     *       CubeControllerTest
     * @param parent the parent ClassLoader to set.
     */
    protected ItSparkClassLoader(ClassLoader parent) throws IOException {
        super(new URL[] {}, parent);
        init();
    }

    public void init() throws MalformedURLException {
        String spark_home = System.getenv("SPARK_HOME");
        if (spark_home == null) {
            spark_home = System.getProperty("SPARK_HOME");
            if (spark_home == null) {
                throw new RuntimeException(
                        "Spark home not found; set it explicitly or use the SPARK_HOME environment variable.");
            }
        }
        File file = new File(spark_home + "/jars");
        File[] jars = file.listFiles();
        for (File jar : jars) {
            addURL(jar.toURI().toURL());
        }
        File sparkJar = findFile("../storage-parquet/target", "kylin-storage-parquet-.*-SNAPSHOT-spark.jar");

        try {
            // sparder and query module has org.apache.spark class ,if not add,
            // that will be load by system classloader
            // (find class api will be find the parent classloader first,
            // so ,parent classloader can not load it ,spark class will not found)
            // why SparkClassLoader is unnecessary?
            // DebugTomcatClassLoader and TomcatClassLoader  find class api will be find itself first
            // so, parent classloader can load it , spark class will be found
            addURL(new File("../engine-spark/target/classes").toURI().toURL());
            addURL(new File("../engine-spark/target/test-classes").toURI().toURL());
            addURL(new File("../storage-parquet/target/classes").toURI().toURL());
            addURL(new File("../query/target/classes").toURI().toURL());
            addURL(new File("../query/target/test-classes").toURI().toURL());
            addURL(new File("../udf/target/classes").toURI().toURL());
            System.setProperty("kylin.query.parquet-additional-jars", sparkJar.getCanonicalPath());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        if (needToUseGlobal(name)) {
            logger.debug("Skipping exempt class " + name + " - delegating directly to parent");
            try {
                return getParent().loadClass(name);
            } catch (ClassNotFoundException e) {
                return super.findClass(name);
            }
        }

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
