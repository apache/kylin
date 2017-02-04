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

import org.apache.catalina.LifecycleException;
import org.apache.catalina.WebResourceRoot;
import org.apache.catalina.loader.ParallelWebappClassLoader;

/**
 * Modified from the openwide-java/tomcat-classloader-ordered in https://github.com/openwide-java/tomcat-classloader-ordered
 *
 * This classloader is designed to return the jar of WEB-INF lib in alphabetical order as it was the case with Tomcat
 * 7.x.
 *
 * See the discussion in https://bz.apache.org/bugzilla/show_bug.cgi?id=57129 for more information.
 */
public class WebappOrderedClassLoader extends ParallelWebappClassLoader {

    public WebappOrderedClassLoader() {
    }

    public WebappOrderedClassLoader(ClassLoader parent) {
        super(parent);
    }

    @Override
    public void setResources(WebResourceRoot resources) {
        super.setResources(new OrderedWebResourceRoot(resources));
    }

    @Override
    public WebappOrderedClassLoader copyWithoutTransformers() {
        WebappOrderedClassLoader result = new WebappOrderedClassLoader(getParent());

        super.copyStateWithoutTransformers(result);

        try {
            result.start();
        } catch (LifecycleException e) {
            throw new IllegalStateException(e);
        }

        return result;
    }

    @Override
    protected Object getClassLoadingLock(String className) {
        return this;
    }
}