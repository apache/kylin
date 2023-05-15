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

import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.model.AntiFlatChecker;
import org.apache.kylin.metadata.model.ColExcludedChecker;
import org.apache.kylin.metadata.model.DeriveInfo;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.SQLDigest;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TableIndexMatcher extends IndexMatcher {

    public TableIndexMatcher(SQLDigest sqlDigest, ChooserContext chooserContext, NDataflow dataflow,
            ColExcludedChecker excludedChecker, AntiFlatChecker antiFlatChecker) {
        super(sqlDigest, chooserContext, dataflow, excludedChecker, antiFlatChecker);
        this.valid = fastValidCheckBeforeMatch();
    }

    @Override
    protected boolean fastValidCheckBeforeMatch() {
        // cols may have null values as the CC col in query may not present in the model
        sqlColumns = sqlDigest.allColumns.stream().map(tblColMap::get).collect(Collectors.toSet());
        return !sqlColumns.contains(null);
    }

    @Override
    public MatchResult match(LayoutEntity layout) {
        if (canSkipIndexMatch(layout.getIndex()) || !isValid()) {
            return new MatchResult();
        }

        log.trace("Matching table index");
        final Map<Integer, DeriveInfo> needDerive = Maps.newHashMap();
        Set<Integer> unmatchedCols = initUnmatchedColumnIds(layout);
        int penaltyFactor = 0;
        if (NProjectManager.getProjectConfig(project).useTableIndexAnswerSelectStarEnabled()) {
            penaltyFactor = unmatchedCols.size();
            unmatchedCols.removeAll(dataflow.getAllColumnsIndex());
            TableRef tableRef = dataflow.getModel().getTableNameMap().get(sqlDigest.factTable);
            if (sqlDigest.isRawQuery && (sqlDigest.allColumns.size() == tableRef.getColumns().size())) {
                unmatchedCols.clear();
            }
        }
        goThruDerivedDims(layout.getIndex(), needDerive, unmatchedCols);
        boolean isMatch = unmatchedCols.isEmpty();
        if (!isMatch) {
            unmatchedCols.removeAll(filterExcludedDims(layout));
            log.debug("After rolling back to TableIndex to match, the unmatched columns are: {}", unmatchedCols);
            isMatch = unmatchedCols.isEmpty();
        }
        if (!isMatch) {
            if (log.isDebugEnabled()) {
                log.debug("Table index {} with unmatched columns {}", layout, unmatchedCols);
            }
            return new MatchResult(false, needDerive,
                    CapabilityResult.IncapableCause.create(CapabilityResult.IncapableType.TABLE_INDEX_MISSING_COLS),
                    Lists.newArrayList());
        }
        return new MatchResult(true, penaltyFactor, needDerive);
    }

    @Override
    protected boolean canSkipIndexMatch(IndexEntity index) {
        boolean isUseTableIndex = dataflow.getConfig().isUseTableIndexAnswerNonRawQuery()
                && !nonSupportFunTableIndex(sqlDigest.aggregations);
        return !index.isTableIndex() || (!sqlDigest.isRawQuery && !isUseTableIndex);
    }
}
