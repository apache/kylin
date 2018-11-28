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

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kylin.common.persistence.ResourceStore.VisitFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResourceParallelCopier {

    private static final Logger logger = LoggerFactory.getLogger(ResourceParallelCopier.class);

    final private ResourceStore src;
    final private ResourceStore dst;

    private int threadCount = 5;
    private int groupSize = 200;
    private int heartBeatSec = 20;
    private int retry = 2;

    public ResourceParallelCopier(ResourceStore src, ResourceStore dst) {
        this.src = src;
        this.dst = dst;
    }

    public void setThreadCount(int threadCount) {
        this.threadCount = threadCount;
    }

    public void setGroupSize(int groupSize) {
        this.groupSize = groupSize;
    }

    public void setHeartBeatSec(int heartBeatSec) {
        this.heartBeatSec = heartBeatSec;
    }

    public void setRetry(int retry) {
        this.retry = retry;
    }

    public Stats copy(String folder, String[] includes, String[] excludes) throws IOException {
        return copy(folder, includes, excludes, new Stats());
    }

    public Stats copy(String folder, String[] includes, String[] excludes, Stats stats) throws IOException {
        logger.info("Copy {} from {} to {}", folder, src, dst);

        TreeMap<String, Integer> groups = calculateGroupsToCopy(folder, includes, excludes);
        if (groups == null || groups.isEmpty())
            return stats;

        copyGroups(groups, includes, excludes, stats);

        while (stats.hasError() && retry > 0) {
            retry--;

            stats.onRetry(stats.errorResource.get());
            copyGroups(collectErrorGroups(stats), includes, excludes, stats);
        }

        logger.info("Done copy {} from {} to {}", folder, src, dst);
        return stats;
    }

    private void copyGroups(TreeMap<String, Integer> groups, String[] includes, String[] excludes, Stats stats) {
        stats.onAllStart(groups);

        // parallel copy all groups
        ExecutorService exec = Executors.newFixedThreadPool(threadCount);
        try {
            doCopyParallel(exec, groups, includes, excludes, stats);
        } finally {
            // await all parallel copy is done
            exec.shutdown();
            stats.heartBeat();
            while (!exec.isTerminated()) {
                try {
                    exec.awaitTermination(heartBeatSec, TimeUnit.SECONDS);
                    stats.heartBeat();
                } catch (InterruptedException e) {
                    logger.error("interruped", e);
                }
            }
        }

        stats.onAllDone();
    }

    private TreeMap<String, Integer> calculateGroupsToCopy(String folder, String[] includes, String[] excludes)
            throws IOException {
        NavigableSet<String> all = src.listResourcesRecursively(folder);
        if (all == null || all.isEmpty())
            return null;

        int sizeBeforeFilter = all.size();

        for (Iterator<String> it = all.iterator(); it.hasNext(); ) {
            String path = it.next();
            if (!ResourceTool.matchFilter(path, includes, excludes)) {
                it.remove();
            }
        }

        int sizeAfterFilter = all.size();
        logger.info("{} resources (out of {}) to copy", sizeAfterFilter, sizeBeforeFilter);

        // returns a list of prefixes, each represents a group of resources
        TreeMap<String, Integer> groupCollector = new TreeMap<>();
        divideGroups(all, "/", groupCollector);
        return groupCollector;
    }

    private TreeMap<String, Integer> collectErrorGroups(Stats stats) {
        TreeMap<String, Integer> newGroups = new TreeMap<>();

        for (String errGroup : stats.errorGroups) {
            newGroups.put(errGroup, stats.allGroups.get(errGroup));
        }
        for (String errResPath : stats.errorResourcePaths) {
            newGroups.put(errResPath, 1);
        }

        return newGroups;
    }

    void divideGroups(NavigableSet<String> resources, String prefixSoFar, TreeMap<String, Integer> groupCollector) {
        if (resources.isEmpty())
            return;
        if (resources.size() <= groupSize) {
            String group = longestCommonPrefix(resources, prefixSoFar);
            groupCollector.put(group, resources.size());
            return;
        }

        // the resources set is too big, divide it
        TreeSet<String> newSet = new TreeSet<>();
        String newPrefix = null;
        int newPrefixLen = prefixSoFar.length() + 1;
        for (String path : resources) {
            String myPrefix = path.length() < newPrefixLen ? path : path.substring(0, newPrefixLen);
            if (newPrefix != null && !myPrefix.equals(newPrefix)) {
                // cut off last group
                divideGroups(newSet, newPrefix, groupCollector);
                newSet.clear();
                newPrefix = null;
            }

            if (newPrefix == null)
                newPrefix = myPrefix;

            newSet.add(path);
        }

        // the last group
        if (!newSet.isEmpty()) {
            divideGroups(newSet, newPrefix, groupCollector);
        }
    }

    String longestCommonPrefix(NavigableSet<String> strs, String prefixSoFar) {
        // find minimal length
        int minLen = Integer.MAX_VALUE;
        for (String s : strs) {
            minLen = Math.min(minLen, s.length());
        }

        for (int i = prefixSoFar.length(); i < minLen; i++) {
            char c = strs.first().charAt(i);
            for (String s : strs) {
                if (s.charAt(i) != c)
                    return s.substring(0, i);
            }
        }

        return strs.first().substring(0, minLen);
    }

    private void doCopyParallel(ExecutorService exec, TreeMap<String, Integer> groups, final String[] includes,
                                final String[] excludes, final Stats stats) {

        for (final Map.Entry<String, Integer> entry : groups.entrySet()) {
            exec.execute(new Runnable() {
                @Override
                public void run() {
                    String group = entry.getKey();
                    int expectResources = entry.getValue();

                    stats.onGroupStart(group);
                    try {
                        int actualResources = copyGroup(group, includes, excludes, stats);
                        stats.onGroupSuccess(group, expectResources, actualResources);
                    } catch (Throwable ex) {
                        stats.onGroupError(group, expectResources, ex);
                    }
                }
            });
        }
    }

    private int copyGroup(String group, final String[] includes, final String[] excludes, final Stats stats)
            throws IOException {

        int cut = group.lastIndexOf('/');
        String folder = cut == 0 ? "/" : group.substring(0, cut);
        final int[] count = new int[1];

        src.visitFolderAndContent(folder, true, new VisitFilter(group), new ResourceStore.Visitor() {
            @Override
            public void visit(RawResource resource) {
                String path = resource.path();
                try {
                    if (!ResourceTool.matchFilter(path, includes, excludes))
                        return;

                    count[0]++;
                    stats.onResourceStart(path);
                    long nBytes = dst.putResource(path, resource.content(), resource.lastModified());
                    stats.onResourceSuccess(path, nBytes);
                } catch (Exception ex) {
                    stats.onResourceError(path, ex);
                } finally {
                    closeQuietly(resource);
                }
            }
        });
        return count[0];
    }

    private void closeQuietly(RawResource raw) {
        try {
            if (raw != null)
                raw.close();
        } catch (Exception e) {
            // ignore
        }
    }

    public static class Stats {

        final public Map<String, Integer> allGroups = Collections.synchronizedMap(new TreeMap<String, Integer>());
        final public Set<String> startedGroups = Collections.synchronizedSet(new TreeSet<String>());
        final public Set<String> successGroups = Collections.synchronizedSet(new TreeSet<String>());
        final public Set<String> errorGroups = Collections.synchronizedSet(new TreeSet<String>());

        final public AtomicLong totalBytes = new AtomicLong();
        final public AtomicInteger totalResource = new AtomicInteger();
        final public AtomicInteger successResource = new AtomicInteger();
        final public AtomicInteger errorResource = new AtomicInteger();
        final public Set<String> errorResourcePaths = Collections.synchronizedSet(new TreeSet<String>());

        public long createTime = System.nanoTime();
        public long startTime;
        public long endTime;

        private void reset() {
            startTime = endTime = 0;
            allGroups.clear();
            startedGroups.clear();
            successGroups.clear();
            errorGroups.clear();
            totalBytes.set(0);
            totalResource.set(0);
            successResource.set(0);
            errorResource.set(0);
            errorResourcePaths.clear();
        }

        void onAllStart(TreeMap<String, Integer> groups) {
            // retry enters here too, reset everything first
            reset();

            logger.debug("{} groups to copy in parallel", groups.size());
            allGroups.putAll(groups);
            startTime = System.nanoTime();
        }

        void onAllDone() {
            endTime = System.nanoTime();
        }

        void onGroupStart(String group) {
            logger.debug("Copying group {}*", group);
            startedGroups.add(group);
        }

        void onGroupError(String group, int resourcesInGroup, Throwable ex) {
            logger.error("Error copying group " + group, ex);
            errorGroups.add(group);
            errorResource.addAndGet(resourcesInGroup);
        }

        void onGroupSuccess(String group, int expectResources, int actualResources) {
            successGroups.add(group);
            if (actualResources != expectResources) {
                logger.warn("Group {} expects {} resources but got {}", group, expectResources, actualResources);
            }
        }

        void onResourceStart(String path) {
            logger.trace("Copying {}", path);
            totalResource.incrementAndGet();
        }

        void onResourceError(String path, Throwable ex) {
            logger.error("Error copying " + path, ex);
            errorResource.incrementAndGet();
            errorResourcePaths.add(path);
        }

        void onResourceSuccess(String path, long nBytes) {
            successResource.incrementAndGet();
            totalBytes.addAndGet(nBytes);
        }

        void onRetry(int errorResourceCnt) {
            // for progress printing
        }

        void heartBeat() {
            // for progress printing
        }

        public boolean hasError() {
            return errorResource.get() > 0;
        }
    }

}
