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
package org.apache.kylin.metadata.cube.cuboid;

import static org.apache.kylin.metadata.model.FunctionDesc.nonSupportFunTableIndex;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kylin.metadata.model.DeriveInfo;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.LayoutEntity;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TableIndexMatcher extends IndexMatcher {

    private final boolean isUseTableIndexAnswerNonRawQuery;
    private Set<Integer> sqlColumns;
    private final boolean valid;

    public TableIndexMatcher(SQLDigest sqlDigest, ChooserContext chooserContext, Set<String> excludedTables,
            boolean isUseTableIndexAnswerNonRawQuery) {
        super(sqlDigest, chooserContext, excludedTables);
        this.isUseTableIndexAnswerNonRawQuery = isUseTableIndexAnswerNonRawQuery;
        valid = init();
    }

    private boolean init() {
        // cols may have null values as the CC col in query may not present in the model
        sqlColumns = sqlDigest.allColumns.stream().map(tblColMap::get).collect(Collectors.toSet());
        return !sqlColumns.contains(null);
    }

    public boolean valid() {
        return valid;
    }

    public MatchResult match(LayoutEntity layout) {
        if (!needTableIndexMatch(layout.getIndex()) || !valid) {
            return new MatchResult(false);
        }

        log.trace("Matching table index");
        final Map<Integer, DeriveInfo> needDerive = Maps.newHashMap();
        Set<Integer> unmatchedCols = Sets.newHashSet();
        unmatchedCols.addAll(sqlColumns);
        if (isBatchFusionModel) {
            unmatchedCols.removeAll(layout.getStreamingColumns().keySet());
        }
        unmatchedCols.removeAll(layout.getOrderedDimensions().keySet());
        goThruDerivedDims(layout.getIndex(), needDerive, unmatchedCols);
        if (!unmatchedCols.isEmpty()) {
            if (log.isDebugEnabled()) {
                log.debug("Table index {} with unmatched columns {}", layout, unmatchedCols);
            }
            return new MatchResult(false, needDerive,
                    CapabilityResult.IncapableCause.create(CapabilityResult.IncapableType.TABLE_INDEX_MISSING_COLS),
                    Lists.newArrayList());
        }
        return new MatchResult(true, needDerive);
    }

    private boolean needTableIndexMatch(IndexEntity index) {
        boolean isUseTableIndex = isUseTableIndexAnswerNonRawQuery && !nonSupportFunTableIndex(sqlDigest.aggregations);
        return index.isTableIndex() && (sqlDigest.isRawQuery || isUseTableIndex);
    }
}
