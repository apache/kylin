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
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.NavigableSet;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceParallelCopier.Stats;
import org.apache.kylin.common.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Sets;

public class ResourceTool {

    private static final Logger logger = LoggerFactory.getLogger(ResourceTool.class);

    private static final Set<String> IMMUTABLE_PREFIX = Sets.newHashSet("/UUID");

    private static final List<String> SKIP_CHILDREN_CHECK_RESOURCE_ROOT = Lists
            .newArrayList(ResourceStore.EXECUTE_RESOURCE_ROOT, ResourceStore.EXECUTE_OUTPUT_RESOURCE_ROOT);

    public static void main(String[] args) throws IOException {
        args = StringUtil.filterSystemArgs(args);

        if (args.length == 0) {
            System.out.println("Usage: ResourceTool list  RESOURCE_PATH");
            System.out.println("Usage: ResourceTool download  LOCAL_DIR [RESOURCE_PATH_PREFIX]");
            System.out.println("Usage: ResourceTool upload    LOCAL_DIR [RESOURCE_PATH_PREFIX]");
            System.out.println("Usage: ResourceTool reset");
            System.out.println("Usage: ResourceTool remove RESOURCE_PATH");
            System.out.println("Usage: ResourceTool cat RESOURCE_PATH");
            return;
        }

        ResourceTool tool = new ResourceTool();

        String include = System.getProperty("include");
        if (include != null) {
            tool.addIncludes(include.split("\\s*,\\s*"));
        }
        String exclude = System.getProperty("exclude");
        if (exclude != null) {
            tool.addExcludes(exclude.split("\\s*,\\s*"));
        }
        String group = System.getProperty("group");
        if (group != null)
            tool.parallelCopyGroupSize = Integer.parseInt(group);

        tool.addExcludes(IMMUTABLE_PREFIX.toArray(new String[IMMUTABLE_PREFIX.size()]));

        String cmd = args[0];
        switch (cmd) {
            case "reset":
                tool.reset(args.length == 1 ? KylinConfig.getInstanceFromEnv() : KylinConfig.createInstanceFromUri(args[1]));
                break;
            case "list":
                tool.list(KylinConfig.getInstanceFromEnv(), args[1]);
                break;
            case "download":
                if (args.length == 2) {
                    tool.copyParallel(KylinConfig.getInstanceFromEnv(), KylinConfig.createInstanceFromUri(args[1]), "/");
                    System.out.println("Metadata backed up to " + args[1]);
                } else if (args.length == 3) {
                    tool.copyParallel(KylinConfig.getInstanceFromEnv(), KylinConfig.createInstanceFromUri(args[1]), args[2]);
                    System.out.println("Metadata with prefix: " + args[2] + " backed up to " + args[1]);
                } else {
                    System.err.println("Illegal args : " + args);
                }
                break;
            case "fetch":
                tool.copy(KylinConfig.getInstanceFromEnv(), KylinConfig.createInstanceFromUri(args[1]), args[2], true);
                break;
            case "upload":
                if (args.length == 2) {
                    tool.copyParallel(KylinConfig.createInstanceFromUri(args[1]), KylinConfig.getInstanceFromEnv(), "/");
                    System.out.println("Metadata restored from " + args[1]);
                } else if (args.length == 3) {
                    tool.copyParallel(KylinConfig.createInstanceFromUri(args[1]), KylinConfig.getInstanceFromEnv(), args[2]);
                    System.out.println("Metadata with prefix: " + args[2] + " restored from " + args[1]);
                } else {
                    System.err.println("Illegal args : " + args);
                }
                break;
            case "remove":
                tool.remove(KylinConfig.getInstanceFromEnv(), args[1]);
                break;
            case "cat":
                tool.cat(KylinConfig.getInstanceFromEnv(), args[1]);
                break;
            default:
                System.out.println("Unknown cmd: " + cmd);
        }
    }

    private String[] includes = null;
    private String[] excludes = null;
    private int parallelCopyGroupSize = 0;

    private void addIncludes(String[] arg) {
        if (arg != null) {
            if (includes != null) {
                String[] nIncludes = new String[includes.length + arg.length];
                System.arraycopy(includes, 0, nIncludes, 0, includes.length);
                System.arraycopy(arg, 0, nIncludes, includes.length, arg.length);
                includes = nIncludes;
            } else {
                includes = arg;
            }
        }
    }

    private void addExcludes(String[] arg) {
        if (arg != null) {
            if (excludes != null) {
                String[] nExcludes = new String[excludes.length + arg.length];
                System.arraycopy(excludes, 0, nExcludes, 0, excludes.length);
                System.arraycopy(arg, 0, nExcludes, excludes.length, arg.length);
                excludes = nExcludes;
            } else {
                excludes = arg;
            }
        }
    }

    private void copyParallel(KylinConfig from, KylinConfig to, String folder) throws IOException {
        ResourceParallelCopier copier = new ResourceParallelCopier(ResourceStore.getStore(from), ResourceStore.getStore(to));
        if (parallelCopyGroupSize > 0)
            copier.setGroupSize(parallelCopyGroupSize);

        Stats stats = copier.copy(folder, includes, excludes, new Stats() {

            @Override
            void heartBeat() {
                double percent = 100D * (successGroups.size() + errorGroups.size()) / allGroups.size();
                double mb = totalBytes.get() / 1024D / 1024D;
                double sec = (System.nanoTime() - startTime) / 1000000000D;
                double kbps = totalBytes.get() / 1024D / sec;
                String status = mb > 0 && kbps < 500 ? "-- Slow network or storage?" : "";

                String logInfo = String.format(Locale.ROOT,
                        "Progress: %2.1f%%, %d resource, %d error; copied %.1f MB in %.1f min, %.1f KB/s %s", percent,
                        totalResource.get(), errorResource.get(), mb, sec / 60, kbps, status);
                System.out.println(logInfo);
            }

            @Override
            void onRetry(int errorResourceCnt) {
                System.out.println("-----");
                System.out.println("RETRY " + errorResourceCnt + " error resource ...");
            }
        });

        if (stats.hasError()) {
            for (String errGroup : stats.errorGroups)
                System.out.println("Failed to copy resource group: " + errGroup + "*");
            for (String errResPath : stats.errorResourcePaths)
                System.out.println("Failed to copy resource: " + errResPath);
            throw new IOException("Failed to copy " + stats.errorResource.get() + " resource");
        }
    }

    public String cat(KylinConfig config, String path) throws IOException {
        ResourceStore store = ResourceStore.getStore(config);
        StringBuffer sb = new StringBuffer();
        String line;

        try (InputStream is = store.getResource(path).content();
                BufferedReader br = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
            while ((line = br.readLine()) != null) {
                System.out.println(line);
                sb.append(line).append('\n');
            }
        }

        return sb.toString();
    }

    public NavigableSet<String> list(KylinConfig config, String path) throws IOException {
        ResourceStore store = ResourceStore.getStore(config);
        NavigableSet<String> result = store.listResources(path);
        System.out.println("" + result);
        return result;
    }

    public void copy(KylinConfig srcConfig, KylinConfig dstConfig, String path) throws IOException {
        copy(srcConfig, dstConfig, path, false);
    }

    //Do NOT invoke this method directly, unless you want to copy and possibly overwrite immutable resources such as UUID.
    public void copy(KylinConfig srcConfig, KylinConfig dstConfig, String path, boolean copyImmutableResource)
            throws IOException {
        ResourceStore src = ResourceStore.getStore(srcConfig);
        ResourceStore dst = ResourceStore.getStore(dstConfig);

        logger.info("Copy from {} to {}", src, dst);

        copyR(src, dst, path, copyImmutableResource);
    }

    public void copy(KylinConfig srcConfig, KylinConfig dstConfig, List<String> paths) throws IOException {
        copy(srcConfig, dstConfig, paths, false);
    }

    //Do NOT invoke this method directly, unless you want to copy and possibly overwrite immutable resources such as UUID.
    public void copy(KylinConfig srcConfig, KylinConfig dstConfig, List<String> paths,
                     boolean copyImmutableResource) throws IOException {
        ResourceStore src = ResourceStore.getStore(srcConfig);
        ResourceStore dst = ResourceStore.getStore(dstConfig);

        logger.info("Copy from {} to {}", src, dst);

        for (String path : paths) {
            copyR(src, dst, path, copyImmutableResource);
        }
    }

    public void copy(KylinConfig srcConfig, KylinConfig dstConfig) throws IOException {
        copy(srcConfig, dstConfig, false);
    }

    //Do NOT invoke this method directly, unless you want to copy and possibly overwrite immutable resources such as UUID.
    public void copy(KylinConfig srcConfig, KylinConfig dstConfig, boolean copyImmutableResource)
            throws IOException {
        copy(srcConfig, dstConfig, "/", copyImmutableResource);
    }

    private void copyR(ResourceStore src, ResourceStore dst, String path, boolean copyImmutableResource)
            throws IOException {

        if (!copyImmutableResource && IMMUTABLE_PREFIX.contains(path)) {
            return;
        }

        boolean isSkip = SKIP_CHILDREN_CHECK_RESOURCE_ROOT.stream()
                .anyMatch(prefixToSkip -> (path.startsWith(prefixToSkip)));
        if (isSkip)
            return;

        NavigableSet<String> children = src.listResources(path);

        if (children == null) {
            // case of resource (not a folder)
            if (matchFilter(path, includes, excludes)) {
                try {
                    RawResource res = src.getResource(path);
                    if (res != null) {
                        logger.debug("Copy path: {} from {} to {}", path, src, dst);
                        try {
                            dst.putResource(path, res.content(), res.lastModified());
                        } finally {
                            res.close();
                        }
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
                copyR(src, dst, child, copyImmutableResource);
        }

    }

    static boolean matchFilter(String path, String[] includePrefix, String[] excludePrefix) {
        if (includePrefix != null) {
            boolean in = false;
            for (String include : includePrefix) {
                in = in || path.startsWith(include);
            }
            if (!in)
                return false;
        }
        if (excludePrefix != null) {
            for (String exclude : excludePrefix) {
                if (path.startsWith(exclude))
                    return false;
            }
        }
        return true;
    }

    public void reset(KylinConfig config) throws IOException {
        ResourceStore store = ResourceStore.getStore(config);
        resetR(store, "/");
    }

    public void resetR(ResourceStore store, String path) throws IOException {
        NavigableSet<String> children = store.listResources(path);
        if (children == null) { // path is a resource (not a folder)
            if (matchFilter(path, includes, excludes)) {
                store.deleteResource(path);
            }
        } else {
            for (String child : children)
                resetR(store, child);
        }
    }

    public void remove(KylinConfig config, String path) throws IOException {
        ResourceStore store = ResourceStore.getStore(config);
        resetR(store, path);
    }
}
