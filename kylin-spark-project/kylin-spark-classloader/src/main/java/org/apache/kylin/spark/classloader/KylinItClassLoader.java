/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kylin.spark.classloader;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.kylin.spark.classloader.ClassLoaderUtils.findFile;

public class KylinItClassLoader extends URLClassLoader {
    private static final String[] PARENT_CL_PRECEDENT_CLASS = new String[] {
            // Java standard library:
            "com.sun.", "launcher.", "javax.", "org.ietf", "java", "org.omg", "org.w3c", "org.xml", "sunw.",
            // logging
            "org.slf4j", "org.apache.commons.logging", "org.apache.log4j", "sun", "org.apache.catalina",
            "org.apache.tomcat",};
    private static final String[] THIS_CL_PRECEDENT_CLASS = new String[] {"io.kyligence", "org.apache.kylin",
            "org.apache.calcite"};
    private static final String[] CODE_GEN_CLASS = new String[] {"org.apache.spark.sql.catalyst.expressions.Object"};
    public static KylinItClassLoader defaultClassLoad = null;
    private static Logger logger = LoggerFactory.getLogger(KylinItClassLoader.class);
    public KylinItSparkClassLoader sparkClassLoader;
    ClassLoader parent;

    /**
     * Creates a DynamicClassLoader that can load classes dynamically
     * from jar files under a specific folder.
     *
     * @param parent the parent ClassLoader to set.
     */
    public KylinItClassLoader(ClassLoader parent) throws IOException {
        super(((URLClassLoader) getSystemClassLoader()).getURLs());
        this.parent = parent;
        sparkClassLoader = new KylinItSparkClassLoader(this);
        ClassLoaderUtils.setSparkClassLoader(sparkClassLoader);
        ClassLoaderUtils.setOriginClassLoader(this);
        defaultClassLoad = this;
        init();
    }

    public void init() {

        String classPath = System.getProperty("java.class.path");
        if (classPath == null) {
            throw new RuntimeException("");
        }

        String[] jars = classPath.split(":");
        for (String jar : jars) {
            if (jar.contains("spark-")) {
                continue;
            }
            try {
                URL url = new File(jar).toURI().toURL();
                addURL(url);
            } catch (MalformedURLException e) {
                e.printStackTrace();
            }
        }
        String spark_home = System.getenv("SPARK_HOME");
        try {
            File sparkJar = findFile(spark_home + "/jars", "spark-yarn_.*.jar");
            addURL(sparkJar.toURI().toURL());
            addURL(new File("../examples/test_case_data/sandbox").toURI().toURL());
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }

    }

    @Override
    public Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        if (isCodeGen(name)) {
            throw new ClassNotFoundException();
        }
        if (name.startsWith("io.kyligence.kap.ext")) {
            return parent.loadClass(name);
        }
        if (isThisCLPrecedent(name)) {
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
                    } catch (ClassNotFoundException e) {
                        // Class not found using this ClassLoader, so delegate to parent
                        logger.debug("Class " + name + " not found - delegating to parent");
                        try {
                            clasz = parent.loadClass(name);
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
        //交换位置 为了让codehua 被父类加载
        if (isParentCLPrecedent(name)) {
            logger.debug("Skipping exempt class " + name + " - delegating directly to parent");
            return parent.loadClass(name);
        }
        if (sparkClassLoader.classNeedPreempt(name)) {
            return sparkClassLoader.loadClass(name);
        }
        return super.loadClass(name, resolve);
    }

    @Override
    public InputStream getResourceAsStream(String name) {
        if (sparkClassLoader.fileNeedPreempt(name)) {
            return sparkClassLoader.getResourceAsStream(name);
        }
        return super.getResourceAsStream(name);

    }

    private boolean isParentCLPrecedent(String name) {
        for (String exemptPrefix : PARENT_CL_PRECEDENT_CLASS) {
            if (name.startsWith(exemptPrefix)) {
                return true;
            }
        }
        return false;
    }

    private boolean isThisCLPrecedent(String name) {
        for (String exemptPrefix : THIS_CL_PRECEDENT_CLASS) {
            if (name.startsWith(exemptPrefix)) {
                return true;
            }
        }
        return false;
    }

    private boolean isCodeGen(String name) {
        for (String exemptPrefix : CODE_GEN_CLASS) {
            if (name.startsWith(exemptPrefix)) {
                return true;
            }
        }
        return false;
    }

}
