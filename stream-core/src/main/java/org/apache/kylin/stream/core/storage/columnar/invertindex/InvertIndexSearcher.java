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

package org.apache.kylin.stream.core.storage.columnar.invertindex;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import org.apache.kylin.metadata.filter.LogicalTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.stream.core.storage.columnar.protocol.CuboidMetaInfo;
import org.apache.kylin.stream.core.storage.columnar.protocol.DimensionMetaInfo;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;

public class InvertIndexSearcher {
    Map<String, ColInvertIndexSearcher> colIndexSearchers = Maps.newHashMap();

    public InvertIndexSearcher(CuboidMetaInfo cuboidMetaInfo, TblColRef[] cols, ByteBuffer idxBuffer)
            throws IOException {
        Map<String, Pair<Integer, Integer>> columnMetas = Maps.newHashMap();
        for (DimensionMetaInfo dimensionInfo : cuboidMetaInfo.getDimensionsInfo()) {
            for (TblColRef col : cols) {
                if (dimensionInfo.getName().equals(col.getName())) {
                    columnMetas
                            .put(col.getName(), new Pair<>(dimensionInfo.getStartOffset() + dimensionInfo.getDataLength(), dimensionInfo.getIndexLength()));
                }
            }
        }
        for (Map.Entry<String, Pair<Integer, Integer>> columnMeta : columnMetas.entrySet()) {
            String colName = columnMeta.getKey();
            Pair<Integer, Integer> positionInfo = columnMeta.getValue();
            int offset = positionInfo.getFirst();
            int length = positionInfo.getSecond();
            //start offset of this column
            ByteBuffer colIdxBuf = idxBuffer.asReadOnlyBuffer();
            colIdxBuf.position(offset);
            colIdxBuf.limit(offset + length);
            ColInvertIndexSearcher colIndexSearcher = ColInvertIndexSearcher.load(colIdxBuf);
            colIndexSearchers.put(colName, colIndexSearcher);
        }
    }

    /**
     *
     * @param tupleFilter
     * @return null if the tupleFilter is null, or filter cannot be applied
     */
    public IndexSearchResult search(TupleFilter tupleFilter) {
        if (tupleFilter == null)
            return null;
        EvalResult evalResult = doEval(tupleFilter);
        IndexSearchResult result = new IndexSearchResult();
        result.allMatch = evalResult.allMatch;
        if (evalResult.bitmap != null) {
            result.rows = evalResult.bitmap.iterator();
        }
        return result;
    }

    public EvalResult doEval(TupleFilter filter) {
        if (filter == null)
            return EvalResult.ALL_MATCH;

        if (filter instanceof LogicalTupleFilter)
            return doEvalLogical((LogicalTupleFilter) filter);

        if (filter instanceof CompareTupleFilter)
            return doEvalCompare((CompareTupleFilter) filter);

        if (filter instanceof ConstantTupleFilter) {
            if (filter.getValues().size() == 0) {
                return new EvalResult();
            } else if (filter.getValues().size() > 0) {
                return EvalResult.ALL_MATCH;
            }
        }

        return EvalResult.ALL_MATCH; // unable to evaluate
    }

    private EvalResult doEvalCompare(CompareTupleFilter filter) {
        switch (filter.getOperator()) {
        case EQ:
            return doEvalCompareEqual(filter);
        case IN:
            return doEvalCompareIn(filter);
        case ISNULL:
            return doEvalCompareIsNull(filter);
        case ISNOTNULL:
            return doEvalCompareIsNotNull(filter);
        case NEQ:
            return doEvalCompareNotEqual(filter);
        case NOTIN:
            return doEvalCompareNotIn(filter);
        case LT:
            return doEvalCompareLT(filter);
        case LTE:
            return doEvalCompareLTE(filter);
        case GT:
            return doEvalCompareGT(filter);
        case GTE:
            return doEvalCompareGTE(filter);
        default:
            throw new IllegalStateException("Unsupported operator " + filter.getOperator());
        }
    }

    private EvalResult doEvalCompareGTE(CompareTupleFilter filter) {
        return EvalResult.ALL_MATCH;
    }

    private EvalResult doEvalCompareGT(CompareTupleFilter filter) {
        return EvalResult.ALL_MATCH;
    }

    private EvalResult doEvalCompareLTE(CompareTupleFilter filter) {
        return EvalResult.ALL_MATCH;
    }

    private EvalResult doEvalCompareLT(CompareTupleFilter filter) {
        return EvalResult.ALL_MATCH;
    }

    private EvalResult doEvalCompareNotIn(CompareTupleFilter filter) {
        return EvalResult.ALL_MATCH;
    }

    private EvalResult doEvalCompareNotEqual(CompareTupleFilter filter) {
        return EvalResult.ALL_MATCH;
    }

    private EvalResult doEvalCompareIsNotNull(CompareTupleFilter filter) {
        return EvalResult.ALL_MATCH;
    }

    private EvalResult doEvalCompareIsNull(CompareTupleFilter filter) {
        EvalResult result = new EvalResult();
        String column = filter.getColumn().getName();
        ColInvertIndexSearcher colSearcher = colIndexSearchers.get(column);
        if (colSearcher == null) {
            return EvalResult.ALL_MATCH;
        }
        ImmutableRoaringBitmap bitmap = colSearcher.searchValue(null);
        if (bitmap != null) {
            result.bitmap = bitmap;
        }
        return result;
    }

    private EvalResult doEvalCompareEqual(CompareTupleFilter filter) {
        EvalResult result = new EvalResult();
        String column = filter.getColumn().getName();
        byte[] value = null;
        if (filter.getFirstValue() instanceof ByteArray) {
            value = ((ByteArray)filter.getFirstValue()).array();
        } else if (filter.getFirstValue() instanceof byte[]) {
            value = (byte[])filter.getFirstValue();
        } else if (filter.getFirstValue() instanceof String) {
            value = Bytes.toBytes((String) filter.getFirstValue());
        }
        ColInvertIndexSearcher colSearcher = colIndexSearchers.get(column);
        if (colSearcher == null) {
            return EvalResult.ALL_MATCH;
        }
        ImmutableRoaringBitmap bitmap = colSearcher.searchValue(value);
        if (bitmap != null) {
            result.bitmap = bitmap;
        }
        return result;
    }

    private EvalResult doEvalCompareIn(CompareTupleFilter filter) {
        EvalResult result = new EvalResult();
        String column = filter.getColumn().getName();
        ColInvertIndexSearcher colSearcher = colIndexSearchers.get(column);
        if (colSearcher == null) {
            return EvalResult.ALL_MATCH;
        }
        List<ImmutableRoaringBitmap> bitmaps = Lists.newArrayList();
        for (Object value : filter.getValues()) {
            byte[] bytes = null;
            if (value instanceof ByteArray) {
                bytes = ((ByteArray)value).array();
            } else if (value instanceof byte[]) {
                bytes = (byte[])value;
            } else if (value instanceof String) {
                bytes = Bytes.toBytes((String)value);
            }
            ImmutableRoaringBitmap bitmap = colSearcher.searchValue(bytes);
            if (bitmap != null) {
                bitmaps.add(bitmap);
            }
        }
        if (bitmaps.isEmpty()) {
            return result;
        }

        result.bitmap = ImmutableRoaringBitmap.or(bitmaps.toArray(new ImmutableRoaringBitmap[bitmaps.size()]));
        return result;
    }

    private EvalResult doEvalLogical(LogicalTupleFilter filter) {
        List<? extends TupleFilter> children = filter.getChildren();

        switch (filter.getOperator()) {
        case AND:
            return doEvalLogicalAnd(children);
        case OR:
            return doEvalLogicalOr(children);
        case NOT:
            return doEvalLogicalNot(children);
        default:
            throw new IllegalStateException("Unsupported operator " + filter.getOperator());
        }
    }

    private EvalResult doEvalLogicalAnd(List<? extends TupleFilter> children) {
        EvalResult result = new EvalResult();
        List<EvalResult> childResults = Lists.newArrayList();
        for (TupleFilter child : children) {
            EvalResult childResult = doEval(child);
            childResults.add(childResult);
            if (childResult.isNoneMatch()) {
                break;
            }
        }
        boolean childrenAllMatched = true;
        for (EvalResult childResult : childResults) {
            if (childResult.isNoneMatch()) {
                return new EvalResult();
            }
            if (childResult.isAllMatch()) {
                continue;
            }
            childrenAllMatched = false;
            ImmutableRoaringBitmap childBitmap = childResult.getBitmap();
            if (result.bitmap == null) {
                result.bitmap = childBitmap;
            } else {
                result.bitmap = ImmutableRoaringBitmap.and(result.bitmap, childBitmap);
            }
        }
        if (childrenAllMatched) {
            result.setAllMatch(true);
        }
        return result;
    }

    private EvalResult doEvalLogicalOr(List<? extends TupleFilter> children) {
        EvalResult result = new EvalResult();
        List<EvalResult> childResults = Lists.newArrayList();
        for (TupleFilter child : children) {
            EvalResult childResult = doEval(child);
            childResults.add(childResult);
            if (childResult.isAllMatch()) {
                break;
            }
        }
        for (EvalResult childResult : childResults) {
            if (childResult.isAllMatch()) {
                return EvalResult.ALL_MATCH;
            }
            if (childResult.isNoneMatch()) {
                continue;
            }
            ImmutableRoaringBitmap childBitmap = childResult.getBitmap();
            if (result.bitmap == null) {
                result.bitmap = childBitmap;
            } else {
                result.bitmap = ImmutableRoaringBitmap.or(result.bitmap, childBitmap);
            }
        }
        return result;
    }

    private EvalResult doEvalLogicalNot(List<? extends TupleFilter> children) {
        return EvalResult.ALL_MATCH;
    }

    private static class EvalResult {
        public static final EvalResult ALL_MATCH = new EvalResult(true, null);
        private boolean allMatch = false;
        private ImmutableRoaringBitmap bitmap;

        public EvalResult() {
            this(false, null);
        }

        public EvalResult(boolean allMatch, ImmutableRoaringBitmap bitmap) {
            this.allMatch = allMatch;
            this.bitmap = bitmap;
        }

        public boolean isAllMatch() {
            return this.allMatch;
        }

        public void setAllMatch(boolean allMatch) {
            this.allMatch = allMatch;
        }

        public boolean isNoneMatch() {
            return !this.allMatch && this.bitmap == null;
        }

        public ImmutableRoaringBitmap getBitmap() {
            return this.bitmap;
        }

        public void setBitmap(ImmutableRoaringBitmap bitmap) {
            this.bitmap = bitmap;
        }
    }

}
