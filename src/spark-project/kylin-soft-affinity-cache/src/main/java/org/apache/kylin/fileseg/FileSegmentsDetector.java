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

package org.apache.kylin.fileseg;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;
import org.apache.kylin.metadata.model.FilePartitionDesc;
import org.apache.kylin.metadata.model.NDataModel;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FileSegmentsDetector {

    private static final ThreadLocal<FileSegmentsDetector> local = new ThreadLocal<>();

    public static void startLocally(String project) {
        local.set(new FileSegmentsDetector(project));
    }

    public static void onObserveFileSystemListFileStatus(Path p, LocatedFileStatus[] content) {
        if (p == null || content == null)
            return;

        // check we are at the driver side; executor side don't have query context
        QueryContext queryContext = QueryContext.current();
        if (queryContext == null)
            return;

        // check we are detecting
        FileSegmentsDetector detector = local.get();
        if (detector == null)
            return;

        detector._onObserveFileSystemListFileStatus(p, content);
    }

    public static void report(Consumer<Finding> findingConsumer) {
        FileSegmentsDetector detector = local.get();
        if (detector == null || findingConsumer == null)
            return;

        try {
            detector._report(findingConsumer);
        } catch (Exception ex) {
            log.error("Tolerate error reporting detected file segments", ex);
        }
    }

    public static void endLocally() {
        local.remove();
    }

    @RequiredArgsConstructor
    public static class Finding {
        public final String project;
        public final String modelId;
        public final String storageLocation;
        public final List<String> fileHashs;
    }

    // =======================================

    private final String project;

    private boolean inited;
    private Map<String, Set<String>> findings;

    public FileSegmentsDetector(String project) {
        this.project = project;
    }

    private void lazyInit() {
        if (inited)
            return;

        findings = new HashMap<>();
        inited = true;
    }

    /*
     * Note: Should only push-down query reaches here. Cube query is NOT intercepted by AbstractCacheFileSystem
     */
    private void _onObserveFileSystemListFileStatus(Path p, LocatedFileStatus[] content) {
        lazyInit();

        Set<String> set = findings.computeIfAbsent(p.toString(), path -> new LinkedHashSet<>());
        for (LocatedFileStatus file : content) {
            set.add(FileSegments.computeFileHash(file));
        }
    }

    private void _report(Consumer<Finding> findingConsumer) {
        List<NDataModel> models = FileSegments.listModelsOfFileSeg(project);
        for (NDataModel model : models) {
            String loc = ((FilePartitionDesc) model.getPartitionDesc()).getFileStorageLocation();
            if (loc == null || !findings.containsKey(loc))
                continue;

            Set<String> fileHashs = findings.get(loc);
            try {
                findingConsumer.accept(new Finding(project, model.getId(), loc, ImmutableList.copyOf(fileHashs)));
            } catch (Exception ex) {
                log.error("Tolerate error while model {} consuming detected file segments {}",
                        project + "/" + model.getAlias(), fileHashs, ex);
            }
        }
    }

}
