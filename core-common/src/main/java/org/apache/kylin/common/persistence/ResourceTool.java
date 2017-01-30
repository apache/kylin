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

package org.apache.kylin.common.persistence;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.NavigableSet;

import org.apache.commons.io.IOUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResourceTool {

    private static String[] includes = null;
    private static String[] excludes = null;
    private static final Logger logger = LoggerFactory.getLogger(ResourceTool.class);

    public static void main(String[] args) throws IOException {
        args = StringUtil.filterSystemArgs(args);

        if (args.length == 0) {
            System.out.println("Usage: ResourceTool list  RESOURCE_PATH");
            System.out.println("Usage: ResourceTool download  LOCAL_DIR");
            System.out.println("Usage: ResourceTool upload    LOCAL_DIR");
            System.out.println("Usage: ResourceTool reset");
            System.out.println("Usage: ResourceTool remove RESOURCE_PATH");
            System.out.println("Usage: ResourceTool cat RESOURCE_PATH");
            return;
        }

        String include = System.getProperty("include");
        if (include != null) {
            setIncludes(include.split("\\s*,\\s*"));
        }
        String exclude = System.getProperty("exclude");
        if (exclude != null) {
            setExcludes(exclude.split("\\s*,\\s*"));
        }

        String cmd = args[0];
        switch (cmd) {
        case "reset":
            reset(args.length == 1 ? KylinConfig.getInstanceFromEnv() : KylinConfig.createInstanceFromUri(args[1]));
            break;
        case "list":
            list(KylinConfig.getInstanceFromEnv(), args[1]);
            break;
        case "download":
            copy(KylinConfig.getInstanceFromEnv(), KylinConfig.createInstanceFromUri(args[1]));
            break;
        case "fetch":
            copy(KylinConfig.getInstanceFromEnv(), KylinConfig.createInstanceFromUri(args[1]), args[2]);
            break;
        case "upload":
            copy(KylinConfig.createInstanceFromUri(args[1]), KylinConfig.getInstanceFromEnv());
            break;
        case "remove":
            remove(KylinConfig.getInstanceFromEnv(), args[1]);
            break;
        case "cat":
            cat(KylinConfig.getInstanceFromEnv(), args[1]);
            break;
        default:
            System.out.println("Unknown cmd: " + cmd);
        }
    }

    public static String[] getIncludes() {
        return includes;
    }

    public static void setIncludes(String[] arg) {
        includes = arg;
    }

    public static String[] getExcludes() {
        return excludes;
    }

    public static void setExcludes(String[] arg) {
        excludes = arg;
    }

    public static String cat(KylinConfig config, String path) throws IOException {
        ResourceStore store = ResourceStore.getStore(config);
        InputStream is = store.getResource(path).inputStream;
        BufferedReader br = null;
        StringBuffer sb = new StringBuffer();
        String line;
        try {
            br = new BufferedReader(new InputStreamReader(is));
            while ((line = br.readLine()) != null) {
                System.out.println(line);
                sb.append(line).append('\n');
            }
        } finally {
            IOUtils.closeQuietly(is);
            IOUtils.closeQuietly(br);
        }
        return sb.toString();
    }

    public static NavigableSet<String> list(KylinConfig config, String path) throws IOException {
        ResourceStore store = ResourceStore.getStore(config);
        NavigableSet<String> result = store.listResources(path);
        System.out.println("" + result);
        return result;
    }

    public static void copy(KylinConfig srcConfig, KylinConfig dstConfig, String path) throws IOException {
        ResourceStore src = ResourceStore.getStore(srcConfig);
        ResourceStore dst = ResourceStore.getStore(dstConfig);

        logger.info("Copy from {} to {}", src, dst);
        
        copyR(src, dst, path);
    }

    public static void copy(KylinConfig srcConfig, KylinConfig dstConfig, List<String> paths) throws IOException {
        ResourceStore src = ResourceStore.getStore(srcConfig);
        ResourceStore dst = ResourceStore.getStore(dstConfig);
        
        logger.info("Copy from {} to {}", src, dst);
        
        for (String path : paths) {
            copyR(src, dst, path);
        }
    }

    public static void copy(KylinConfig srcConfig, KylinConfig dstConfig) throws IOException {
        copy(srcConfig, dstConfig, "/");
    }

    public static void copyR(ResourceStore src, ResourceStore dst, String path) throws IOException {
        NavigableSet<String> children = src.listResources(path);

        if (children == null) {
            // case of resource (not a folder)
            if (matchFilter(path)) {
                try {
                    RawResource res = src.getResource(path);
                    if (res != null) {
                        dst.putResource(path, res.inputStream, res.timestamp);
                        res.inputStream.close();
                    } else {
                        System.out.println("Resource not exist for " + path);
                    }
                } catch (Exception ex) {
                    System.err.println("Failed to open " + path);
                    logger.error(ex.getLocalizedMessage(), ex);
                }
            }
        } else {
            // case of folder
            for (String child : children)
                copyR(src, dst, child);
        }
    }

    private static boolean matchFilter(String path) {
        if (includes != null) {
            boolean in = false;
            for (String include : includes) {
                in = in || path.startsWith(include);
            }
            if (!in)
                return false;
        }
        if (excludes != null) {
            for (String exclude : excludes) {
                if (path.startsWith(exclude))
                    return false;
            }
        }
        return true;
    }

    public static void reset(KylinConfig config) throws IOException {
        ResourceStore store = ResourceStore.getStore(config);
        resetR(store, "/");
    }

    public static void resetR(ResourceStore store, String path) throws IOException {
        NavigableSet<String> children = store.listResources(path);
        if (children == null) { // path is a resource (not a folder)
            if (matchFilter(path)) {
                store.deleteResource(path);
            }
        } else {
            for (String child : children)
                resetR(store, child);
        }
    }

    private static void remove(KylinConfig config, String path) throws IOException {
        ResourceStore store = ResourceStore.getStore(config);
        resetR(store, path);
    }
}
