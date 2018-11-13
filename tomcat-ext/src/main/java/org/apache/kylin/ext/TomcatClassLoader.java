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

import org.apache.catalina.loader.ParallelWebappClassLoader;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.util.HashSet;
import java.util.Set;

import static org.apache.kylin.ext.ClassLoaderUtils.findFile;

public class TomcatClassLoader extends ParallelWebappClassLoader {
    private static String[] PARENT_CL_PRECEDENT_CLASSES = new String[] {
            // Java standard library:
            "com.sun.", "launcher.", "javax.", "org.ietf", "java", "org.omg", "org.w3c", "org.xml", "sunw.",
            // logging
            "org.slf4j", "org.apache.commons.logging", "org.apache.log4j", "org.apache.catalina", "org.apache.tomcat" };

    private static String[] THIS_CL_PRECEDENT_CLASSES = new String[] { "org.apache.kylin",
            "org.apache.calcite" };

    private static String[] CODEGEN_CLASSES = new String[] { "org.apache.spark.sql.catalyst.expressions.Object",
            "Baz" };

    private static final Set<String> wontFindClasses = new HashSet<>();

    static {
        String tomcatclassloaderParentClPrecedentClasses = System
                .getenv("TOMCATCLASSLOADER_PARENT_CL_PRECEDENT_CLASSES");
        if (!StringUtils.isEmpty(tomcatclassloaderParentClPrecedentClasses)) {
            PARENT_CL_PRECEDENT_CLASSES = StringUtils.split(tomcatclassloaderParentClPrecedentClasses, ",");
        }

        String tomcatclassloaderThisClPrecedentClasses = System
                .getenv("TOMCATCLASSLOADER_THIS_CL_PRECEDENT_CLASSES");
        if (!StringUtils.isEmpty(tomcatclassloaderThisClPrecedentClasses)) {
            THIS_CL_PRECEDENT_CLASSES = StringUtils.split(tomcatclassloaderThisClPrecedentClasses, ",");
        }

        String tomcatclassloaderCodegenClasses = System.getenv("TOMCATCLASSLOADER_CODEGEN_CLASSES");
        if (!StringUtils.isEmpty(tomcatclassloaderCodegenClasses)) {
            CODEGEN_CLASSES = StringUtils.split(tomcatclassloaderCodegenClasses, ",");
        }

        wontFindClasses.add("Class");
        wontFindClasses.add("Object");
        wontFindClasses.add("org");
        wontFindClasses.add("java.lang.org");
        wontFindClasses.add("java.lang$org");
        wontFindClasses.add("java$lang$org");
        wontFindClasses.add("org.apache");
        wontFindClasses.add("org.apache.calcite");
        wontFindClasses.add("org.apache.calcite.runtime");
        wontFindClasses.add("org.apache.calcite.linq4j");
        wontFindClasses.add("Long");
        wontFindClasses.add("String");
    }

    public static TomcatClassLoader defaultClassLoad = null;
    private static Logger logger = LoggerFactory.getLogger(TomcatClassLoader.class);
    public SparkClassLoader sparkClassLoader;

    /**
     * Creates a DynamicClassLoader that can load classes dynamically
     * from jar files under a specific folder.
     *
     * @param parent the parent ClassLoader to set.
     */
    public TomcatClassLoader(ClassLoader parent) throws IOException {
        super(parent);
        sparkClassLoader = new SparkClassLoader(this);
        ClassLoaderUtils.setSparkClassLoader(sparkClassLoader);
        ClassLoaderUtils.setOriginClassLoader(this);
        defaultClassLoad = this;
        init();
    }

    @SuppressWarnings("checkstyle:LocalVariableName")
    public void init() {
        String spark_home = System.getenv("SPARK_HOME");
        try {
            //  SparkContext use spi to match deploy mode
            //  otherwise SparkContext init fail ,can not find yarn deploy mode
            File yarnJar = findFile(spark_home + "/jars", "spark-yarn.*.jar");
            addURL(yarnJar.toURI().toURL());
            //  jersey in spark will attempt find @Path class file in current classloader.
            // Not possible to delegate to spark loader
            // otherwise spark web ui executors tab can not render
            File coreJar = findFile(spark_home + "/jars", "spark-core.*.jar");
            addURL(coreJar.toURI().toURL());
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }

    }

    @Override
    public Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        // when calcite compile class, some stupid class name will be proposed, not worth to actually lookup
        if (isWontFind(name)) {
            throw new ClassNotFoundException();
        }
        // spark codegen classload parent is Thread.currentThread().getContextClassLoader()
        // and calcite baz classloader is EnumerableInterpretable.class's classloader
        if (isCodeGen(name)) {
            throw new ClassNotFoundException();
        }
        // class loaders should conform to global's
        if (name.startsWith("org.apache.kylin.ext")) {
            return parent.loadClass(name);
        }

        if (name.startsWith("org.apache.spark.sql.execution.datasources.sparder.batch.SparderBatchFileFormat")) {
            return super.loadClass(name, resolve);
        }

        // if spark CL needs preempt
        if (sparkClassLoader.classNeedPreempt(name)) {
            return sparkClassLoader.loadClass(name);
        }
        // tomcat classpath include KAP_HOME/lib , ensure this classload can load kap class
        if (isParentCLPrecedent(name) && !isThisCLPrecedent(name)) {
            logger.debug("delegate " + name + " directly to parent");
            return parent.loadClass(name);
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
        for (String exemptPrefix : PARENT_CL_PRECEDENT_CLASSES) {
            if (name.startsWith(exemptPrefix)) {
                return true;
            }
        }
        return false;
    }

    private boolean isThisCLPrecedent(String name) {
        for (String exemptPrefix : THIS_CL_PRECEDENT_CLASSES) {
            if (name.startsWith(exemptPrefix)) {
                return true;
            }
        }
        return false;
    }

    private boolean isWontFind(String name) {
        return wontFindClasses.contains(name);
    }

    private boolean isCodeGen(String name) {
        for (String exemptPrefix : CODEGEN_CLASSES) {
            if (name.startsWith(exemptPrefix)) {
                return true;
            }
        }
        return false;
    }
}
