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

import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import io.kyligence.kap.secondstorage.SecondStorageNodeHelper;
import io.kyligence.kap.secondstorage.SecondStorageQueryRouteUtil;
import io.kyligence.kap.secondstorage.util.SecondStorageDateUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kylin.metadata.model.SegmentRange;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class ClickHouseTableStorageMetric {
    private List<String> nodes;
    private boolean load = false;
    private Map<String, List<ClickHouseSystemQuery.PartitionSize>> partitionSize;

    public ClickHouseTableStorageMetric(List<String> nodes) {
        this.nodes = nodes;
    }

    public void collect(boolean skipDownNode) {
        if (load) return;
        partitionSize = nodes.parallelStream().collect(Collectors.toMap(node -> node,
                node -> {
                    if (skipDownNode && !SecondStorageQueryRouteUtil.getNodeStatus(node)) {
                        return Collections.emptyList();
                    }
                    try (ClickHouse clickHouse = new ClickHouse(SecondStorageNodeHelper.resolve(node))) {
                        return clickHouse.query(ClickHouseSystemQuery.queryTableStorageSize(), ClickHouseSystemQuery.TABLE_STORAGE_MAPPER);
                    } catch (SQLException sqlException) {
                        return ExceptionUtils.rethrow(sqlException);
                    }
                }));
        load = true;
    }

    public Map<String, Long> getByPartitions(String database, String table, SegmentRange segmentRange, String dateFormat) {
        Map<String, Long> sizeInNode;
        if (segmentRange.isInfinite()) {
            sizeInNode = this.getByPartitions(database, table, Collections.singletonList("tuple()"));
        } else {
            SimpleDateFormat partitionFormat = new SimpleDateFormat(dateFormat, Locale.getDefault(Locale.Category.FORMAT));
            sizeInNode = this.getByPartitions(database, table,
                    SecondStorageDateUtils.splitByDay((SegmentRange<Long>) segmentRange).stream()
                            .map(partitionFormat::format).collect(Collectors.toList()));
        }
        return sizeInNode;
    }

    public Map<String, Long> getByPartitions(String database, String table, List<String> partitions) {
        Preconditions.checkArgument(load);
        Set<String> partitionSet = new HashSet<>(partitions);
        return nodes.stream().collect(Collectors.toMap(node -> node,
                node -> sumNodeTableSize(node, database, table, partitionSet)));
    }

    private long sumNodeTableSize(String node, String database, String table, Set<String> partitionSet) {
        return partitionSet.stream()
                .flatMap(partition -> partitionSize.get(node).stream()
                        .filter(size -> Objects.equals(database, size.getDatabase())
                                && Objects.equals(table, size.getTable())
                                && Objects.equals(partition, size.getPartition()))
                        .map(ClickHouseSystemQuery.PartitionSize::getBytes))
                .reduce(Long::sum).orElse(0L);
    }
}
