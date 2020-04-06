/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.apache.kylin.spark.classloader;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashSet;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.kylin.spark.classloader.ClassLoaderUtils.findFile;


public class KylinItSparkClassLoader extends URLClassLoader {
    private static final String[] SPARK_CL_PREEMPT_CLASSES = new String[] {"org.apache.spark", "scala.",
            "org.spark_project"
            //            "javax.ws.rs.core.Application",
            //            "javax.ws.rs.core.UriBuilder", "org.glassfish.jersey", "javax.ws.rs.ext"
            //user javax.ws.rs.api 2.01  not jersey-core-1.9.jar
    };
    private static final String[] SPARK_CL_PREEMPT_FILES = new String[] {"spark-version-info.properties",
            "HiveClientImpl", "org/apache/spark"};

    private static final String[] THIS_CL_PRECEDENT_CLASSES = new String[] {"javax.ws.rs", "org.apache.hadoop.hive"};

    private static final String[] PARENT_CL_PRECEDENT_CLASSES = new String[] {
            //            // Java standard library:
            "com.sun.", "launcher.", "java.", "javax.", "org.ietf", "org.omg", "org.w3c", "org.xml", "sunw.", "sun.",
            // logging
            "org.apache.commons.logging", "org.apache.log4j", "com.hadoop", "org.slf4j",
            // Hadoop/ZK:
            "org.apache.hadoop", "org.apache.zookeeper", "io.kyligence", "org.apache.kylin", "com.intellij",
            "org.apache.calcite", "org.roaringbitmap", "org.apache.parquet"};
    private static final Set<String> classNotFoundCache = new HashSet<>();
    private static Logger logger = LoggerFactory.getLogger(KylinItSparkClassLoader.class);

    /**
     * Creates a DynamicClassLoader that can load classes dynamically
     * from jar files under a specific folder.
     * CubeControllerTest
     *
     * @param parent the parent ClassLoader to set.
     */
    protected KylinItSparkClassLoader(ClassLoader parent) throws IOException {
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
        File sparkFile = findFile("../../build/conf", "spark-executor-log4j.properties");

        try {
            // sparder and query module has org.apache.spark class ,if not add,
            // that will be load by system classloader
            // (find class api will be find the parent classloader first,
            // so ,parent classloader can not load it ,spark class will not found)
            // why SparkClassLoader is unnecessary?
            // DebugTomcatClassLoader and TomcatClassLoader  find class api will be find itself first
            // so, parent classloader can load it , spark class will be found
            addURL(new File("../sparder/target/classes").toURI().toURL());
            addURL(new File("../storage-parquet/target/classes").toURI().toURL());
            addURL(new File("../query/target/classes").toURI().toURL());
            addURL(new File("../query/target/test-classes").toURI().toURL());
            addURL(new File("../udf/target/classes").toURI().toURL());
            System.setProperty("kap.query.engine.sparder-additional-files", sparkFile.getCanonicalPath());
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
