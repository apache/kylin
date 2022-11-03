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

package io.kyligence.kap.clickhouse.job;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import org.apache.kylin.common.util.JsonUtil;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

@ThreadSafe
public class LoadContext {
    public static final String CLICKHOUSE_LOAD_CONTEXT = "P_CLICKHOUSE_LOAD_CONTEXT";

    private final ConcurrentMap<String, List<String>> completedSegments;
    private final ConcurrentMap<String, List<String>> completedFiles;
    private final ConcurrentMap<String, List<String>> history;
    private final ConcurrentMap<String, List<String>> historySegments;
    private final ClickHouseLoad job;

    public LoadContext(ClickHouseLoad job) {
        completedFiles = new ConcurrentHashMap<>();
        completedSegments = new ConcurrentHashMap<>();
        history = new ConcurrentHashMap<>();
        historySegments = new ConcurrentHashMap<>();
        this.job = job;
    }

    public void finishSingleFile(CompletedFileKeyUtil keyUtil, String file) {
        completedFiles.computeIfAbsent(keyUtil.toKey(), key -> new CopyOnWriteArrayList<>()).add(file);
    }

    public void finishSegment(String segment, CompletedSegmentKeyUtil keyUtil) {
        completedSegments.computeIfAbsent(keyUtil.toKey(), key -> new CopyOnWriteArrayList<>()).add(segment);
    }

    public List<String> getHistory(CompletedFileKeyUtil keyUtil) {
        return Collections.unmodifiableList(this.history.getOrDefault(keyUtil.toKey(), Collections.emptyList()));
    }

    public Map<String, List<String>> getHistory() {
        return Collections.unmodifiableMap(this.history);
    }
    
    public List<String> getHistorySegments(CompletedSegmentKeyUtil keyUtil) {
        return Collections.unmodifiableList(this.historySegments.getOrDefault(keyUtil.toKey(), Collections.emptyList()));
    }

    @SneakyThrows
    public String serializeToString() {
        return JsonUtil.writeValueAsString(new ContextDump(completedSegments, completedFiles));
    }

    @SneakyThrows
    public static String emptyState() {
        return JsonUtil.writeValueAsString(ContextDump.getEmptyInstance());
    }

    @SneakyThrows
    public void deserializeToString(String state) {
        ContextDump historyState = JsonUtil.readValue(state, ContextDump.class);
        completedFiles.clear();
        history.clear();
        completedSegments.clear();
        historySegments.clear();

        history.putAll(historyState.getCompletedFiles() == null ? Collections.emptyMap() : historyState.getCompletedFiles());
        historySegments.putAll(historyState.getCompletedSegments() == null ? Collections.emptyMap() : historyState.getCompletedSegments());
        completedFiles.putAll(history);
        completedSegments.putAll(historySegments);
    }

    public ClickHouseLoad getJob() {
        return this.job;
    }

    public boolean isNewJob() {
        return history.isEmpty();
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    static class ContextDump {
        private Map<String, List<String>> completedSegments; // CompletedSegmentKeyUtil.toKey / value: segment_ids
        private Map<String, List<String>> completedFiles; // kes: CompletedFileKeyUtil.toKey / value: hdfs files

        static ContextDump getEmptyInstance() {
            return new ContextDump(Collections.emptyMap(), Collections.emptyMap());
        }
    }

    @AllArgsConstructor
    public static class CompletedFileKeyUtil {
        private final String shardName;
        private final Long layoutId;

        public String toKey() {
            return this.shardName + "_" + layoutId;
        }
    }

    @AllArgsConstructor
    public static class CompletedSegmentKeyUtil {
        private final Long layoutId;

        public String toKey() {
            return String.valueOf(layoutId);
        }
    }
}
