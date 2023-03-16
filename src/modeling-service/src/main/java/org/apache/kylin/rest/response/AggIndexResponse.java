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
package org.apache.kylin.rest.response;

import static org.apache.kylin.metadata.cube.model.IndexEntity.Range.BATCH;
import static org.apache.kylin.metadata.cube.model.IndexEntity.Range.HYBRID;
import static org.apache.kylin.metadata.cube.model.IndexEntity.Range.STREAMING;

import java.io.Serializable;
import java.util.List;

import org.apache.kylin.metadata.cube.model.IndexEntity;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kylin.guava30.shaded.common.collect.Lists;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.val;

@Getter
@AllArgsConstructor
public class AggIndexResponse implements Serializable {

    private static AggIndexResponse EMPTY = new AggIndexResponse(Lists.newArrayList(),
            AggIndexCombResult.successResult(0), 0L);

    @JsonProperty(value = "agg_index_counts")
    private List<AggIndexCombResult> aggIndexCounts;
    @JsonProperty(value = "total_count")
    private AggIndexCombResult totalCount;
    @JsonProperty(value = "max_combination_num")
    private Long aggrgroupMaxCombination;

    public static AggIndexResponse combine(AggIndexResponse batch, AggIndexResponse stream,
            List<IndexEntity.Range> aggGroupTypes) {
        if (batch.isEmpty()) {
            return stream;
        }
        if (stream.isEmpty()) {
            return batch;
        }
        val combineTotalCount = AggIndexCombResult.combine(batch.getTotalCount(), stream.getTotalCount());
        val aggIndexCounts = Lists.<AggIndexCombResult> newArrayList();

        int batchIndex = 0;
        int streamIndex = 0;
        for (int n = 0; n < aggGroupTypes.size(); n++) {
            if (aggGroupTypes.get(n) == BATCH) {
                aggIndexCounts.add(batch.getAggIndexCounts().get(batchIndex++));
            } else if (aggGroupTypes.get(n) == STREAMING) {
                aggIndexCounts.add(stream.getAggIndexCounts().get(streamIndex++));
            } else if (aggGroupTypes.get(n) == HYBRID) {
                aggIndexCounts.add(AggIndexCombResult.combine(batch.getAggIndexCounts().get(batchIndex++),
                        stream.getAggIndexCounts().get(streamIndex++)));
            }

        }
        AggIndexResponse combineResponse = new AggIndexResponse(aggIndexCounts, combineTotalCount,
                stream.getAggrgroupMaxCombination());
        return combineResponse;
    }

    private boolean isEmpty() {
        return EMPTY == this;
    }

    public static AggIndexResponse empty() {
        return EMPTY;
    }
}
