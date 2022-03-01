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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;
import org.apache.catalina.loader.ParallelWebappClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TomcatClassLoader extends ParallelWebappClassLoader {
    private static final String[] PARENT_CL_PRECEDENT_CLASSES = new String[] {
            // Java standard library:
            "com.sun.", "launcher.", "javax.", "org.ietf", "java", "org.omg", "org.w3c", "org.xml", "sunw.",
            // logging
            "org.slf4j", "org.apache.commons.logging", "org.apache.log4j", "org.apache.catalina", "org.apache.tomcat"};
    private static final String[] THIS_CL_PRECEDENT_CLASSES = new String[] {"org.apache.kylin",
            "org.apache.calcite"};
    private static final String[] CODE_GEN_CLASS = new String[] {"org.apache.spark.sql.catalyst.expressions.Object",
            "Baz"};

    private static final Set<String> wontFindClasses = new HashSet<>();

    static {
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

    private static Logger logger = LoggerFactory.getLogger(TomcatClassLoader.class);

    /**
     * Creates a DynamicClassLoader that can load classes dynamically
     * from jar files under a specific folder.
     *
     * @param parent the parent ClassLoader to set.
     */
    public TomcatClassLoader(ClassLoader parent) throws IOException {
        super(parent);
        ClassLoaderUtils.setOriginClassLoader(this);
        init();
    }

    public void init() {

        String classPath = System.getProperty("java.class.path");
        if (classPath == null) {
            throw new RuntimeException("");
        }

        String[] jars = classPath.split(":");
        for (String jar : jars) {
            try {
                URL url = new File(jar).toURI().toURL();
                addURL(url);
            } catch (MalformedURLException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        if (isWontFind(name)) {
            throw new ClassNotFoundException();
        }
        if (isCodeGen(name)) {
            throw new ClassNotFoundException();
        }

        if (name.startsWith("org.apache.kylin.spark.classloader")) {
            return parent.loadClass(name);
        }
        if (isParentCLPrecedent(name)) {
            logger.debug("Skipping exempt class " + name + " - delegating directly to parent");
            return parent.loadClass(name);
        }
        return super.loadClass(name, resolve);
    }

    @Override
    public InputStream getResourceAsStream(String name) {
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

    private boolean isWontFind(String name) {
        return wontFindClasses.contains(name);
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
