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

package org.apache.kylin.loader;

import java.net.URL;
import java.security.PrivilegedAction;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

import org.apache.kylin.loader.utils.ClassLoaderUtils;

/**
 * Reference from hive org.apache.hadoop.hive.ql.exec.AddToClassPathAction
 */
public class AddToClassPathAction implements PrivilegedAction<ParserClassLoader> {

    private final ClassLoader parentLoader;
    private final Collection<String> newPaths;
    private final boolean forceNewClassLoader;

    public AddToClassPathAction(ClassLoader parentLoader, Collection<String> newPaths, boolean forceNewClassLoader) {
        this.parentLoader = parentLoader;
        this.newPaths = newPaths != null ? newPaths : Collections.emptyList();
        this.forceNewClassLoader = forceNewClassLoader;
        if (Objects.isNull(parentLoader)) {
            throw new IllegalArgumentException("ParserClassLoader is not designed to be a bootstrap class loader!");
        }
    }

    public AddToClassPathAction(ClassLoader parentLoader, Collection<String> newPaths) {
        this(parentLoader, newPaths, false);
    }

    @Override
    public ParserClassLoader run() {
        if (useExistingClassLoader()) {
            final ParserClassLoader parserClassLoader = (ParserClassLoader) this.parentLoader;
            for (String path : newPaths) {
                parserClassLoader.addURL(ClassLoaderUtils.urlFromPathString(path));
            }
            return parserClassLoader;
        }
        return createParserClassLoader();
    }

    private boolean useExistingClassLoader() {
        if (!forceNewClassLoader && parentLoader instanceof ParserClassLoader) {
            final ParserClassLoader parserClassLoader = (ParserClassLoader) this.parentLoader;
            return !parserClassLoader.isClosed();
        }
        return false;
    }

    private ParserClassLoader createParserClassLoader() {
        return new ParserClassLoader(newPaths.stream().map(ClassLoaderUtils::urlFromPathString).toArray(URL[]::new),
                parentLoader);
    }
}
