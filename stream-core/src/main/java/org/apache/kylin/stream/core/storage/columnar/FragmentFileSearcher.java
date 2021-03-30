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

package org.apache.kylin.stream.core.storage.columnar;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.cube.kv.CubeDimEncMap;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.dict.BuiltInFunctionTransformer;
import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.dimension.IDimensionEncodingMap;
import org.apache.kylin.metadata.filter.ColumnTupleFilter;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import org.apache.kylin.metadata.filter.FilterOptimizeTransformer;
import org.apache.kylin.metadata.filter.ITupleFilterTransformer;
import org.apache.kylin.metadata.filter.LogicalTupleFilter;
import org.apache.kylin.metadata.filter.StringCodeSystem;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter.FilterOperatorEnum;
import org.apache.kylin.metadata.filter.TupleFilterSerializer;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.stream.core.query.IStreamingGTSearcher;
import org.apache.kylin.stream.core.query.IStreamingSearchResult;
import org.apache.kylin.stream.core.query.ResponseResultSchema;
import org.apache.kylin.stream.core.query.ResultCollector;
import org.apache.kylin.stream.core.query.StreamingSearchContext;
import org.apache.kylin.stream.core.storage.columnar.protocol.CuboidMetaInfo;
import org.apache.kylin.stream.core.storage.columnar.protocol.FragmentMetaInfo;
import org.apache.kylin.stream.core.util.CompareFilterTimeRangeChecker;
import org.apache.kylin.stream.core.util.CompareFilterTimeRangeChecker.CheckResult;
import org.apache.kylin.dimension.TimeDerivedColumnType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.apache.kylin.shaded.com.google.common.collect.Sets;

/**
 * FragmentFileSearcher is responsible to scan the columnar based storage on disk and deal with all the bytes level details for each DataFragment and return the result as GTRecords.
 * 
 */
public class FragmentFileSearcher implements IStreamingGTSearcher {
    private static Logger logger = LoggerFactory.getLogger(FragmentFileSearcher.class);

    private FragmentData fragmentData;
    private DataSegmentFragment fragment;

    public FragmentFileSearcher(DataSegmentFragment fragment, FragmentData fragmentData) {
        this.fragment = fragment;
        this.fragmentData = fragmentData;
    }

    @Override
    public void search(StreamingSearchContext searchContext, ResultCollector collector) throws IOException {
        String timezone = searchContext.getCubeDesc().getConfig().getStreamingDerivedTimeTimezone();
        long timezoneOffset = 0;
        if (timezone != null && timezone.length() > 0) {
            timezoneOffset = TimeZone.getTimeZone(timezone).getRawOffset();
        }
        FragmentMetaInfo fragmentMetaInfo = fragmentData.getFragmentMetaInfo();
        CuboidMetaInfo cuboidMetaInfo;
        if (searchContext.hitBasicCuboid()) {
            cuboidMetaInfo = fragmentMetaInfo.getBasicCuboidMetaInfo();
        } else {
            cuboidMetaInfo = fragmentMetaInfo.getCuboidMetaInfo(searchContext.getHitCuboid());
            if (cuboidMetaInfo == null) {
                logger.warn("the cuboid:{} is not exist in the fragment:{}, use basic cuboid instead",
                        searchContext.getHitCuboid(), fragment.getFragmentId());
                cuboidMetaInfo = fragmentMetaInfo.getBasicCuboidMetaInfo();
            }
        }

        ResponseResultSchema responseSchema = searchContext.getRespResultSchema();
        TblColRef[] dimensions = responseSchema.getDimensions();
        FunctionDesc[] metrics = responseSchema.getMetrics();
        Map<TblColRef, Dictionary<String>> dictMap = fragmentData.getDimensionDictionaries(dimensions);

        CubeDesc cubeDesc = responseSchema.getCubeDesc();
        List<MeasureDesc> allMeasures = cubeDesc.getMeasures();
        Map<FunctionDesc, MeasureDesc> funcMeasureMap = Maps.newHashMap();
        for (MeasureDesc measure : allMeasures) {
            funcMeasureMap.put(measure.getFunction(), measure);
        }
        MeasureDesc[] measures = new MeasureDesc[metrics.length];
        for (int i = 0; i < measures.length; i++) {
            measures[i] = funcMeasureMap.get(metrics[i]);
        }
        DimensionEncoding[] dimensionEncodings = ParsedStreamingCubeInfo.getDimensionEncodings(cubeDesc, dimensions,
                dictMap);
        ColumnarMetricsEncoding[] metricsEncodings = ParsedStreamingCubeInfo.getMetricsEncodings(measures);
        ColumnarRecordCodec recordCodec = new ColumnarRecordCodec(dimensionEncodings, metricsEncodings);

        // change the unEvaluable dimensions to groupBy
        Set<TblColRef> unEvaluateDims = Sets.newHashSet();
        TupleFilter fragmentFilter = null;
        if (searchContext.getFilter() != null) {
            fragmentFilter = convertFilter(fragmentMetaInfo, searchContext.getFilter(), recordCodec, dimensions,
                    new CubeDimEncMap(cubeDesc, dictMap), unEvaluateDims, timezoneOffset);
        }
        if (ConstantTupleFilter.TRUE == fragmentFilter) {
            fragmentFilter = null;
        } else if (ConstantTupleFilter.FALSE == fragmentFilter) {
            collector.collectSearchResult(IStreamingSearchResult.EMPTY_RESULT);
        }
        Set<TblColRef> groups = searchContext.getGroups();
        if (!unEvaluateDims.isEmpty()) {
            searchContext.addNewGroups(unEvaluateDims);
            groups = Sets.union(groups, unEvaluateDims);
        }
        collector.collectSearchResult(new FragmentSearchResult(fragment, fragmentData, cuboidMetaInfo, responseSchema, fragmentFilter, groups, searchContext.getHavingFilter(),
                recordCodec));
    }

    private TupleFilter convertFilter(FragmentMetaInfo fragmentMetaInfo, TupleFilter rootFilter,
            ColumnarRecordCodec recordCodec, final TblColRef[] dimensions, final IDimensionEncodingMap dimEncodingMap, //
            final Set<TblColRef> unEvaluableColumnCollector, long timezoneOffset) {
        Map<TblColRef, Integer> colMapping = Maps.newHashMap();
        for (int i = 0; i < dimensions.length; i++) {
            colMapping.put(dimensions[i], i);
        }
        byte[] bytes = TupleFilterSerializer.serialize(rootFilter, null, StringCodeSystem.INSTANCE);
        TupleFilter filter = TupleFilterSerializer.deserialize(bytes, StringCodeSystem.INSTANCE);

        BuiltInFunctionTransformer builtInFunctionTransformer = new BuiltInFunctionTransformer(dimEncodingMap);
        filter = builtInFunctionTransformer.transform(filter);
        FragmentFilterConverter fragmentFilterConverter = new FragmentFilterConverter(fragmentMetaInfo, unEvaluableColumnCollector,
                colMapping, recordCodec);
        fragmentFilterConverter.setTimezoneOffset(timezoneOffset);
        filter = fragmentFilterConverter.transform(filter);

        filter = new FilterOptimizeTransformer().transform(filter);
        return filter;
    }

    protected static class FragmentFilterConverter implements ITupleFilterTransformer {
        protected final Set<TblColRef> unEvaluableColumnCollector;
        protected final Map<TblColRef, Integer> colMapping;
        private CompareFilterTimeRangeChecker filterTimeRangeChecker;
        private ColumnarRecordCodec recordCodec;
        transient ByteBuffer buf;
        private long timezoneOffset = 0;

        public FragmentFilterConverter(FragmentMetaInfo fragmentMetaInfo, Set<TblColRef> unEvaluableColumnCollector,
                                       Map<TblColRef, Integer> colMapping, ColumnarRecordCodec recordCodec) {
            this.unEvaluableColumnCollector = unEvaluableColumnCollector;
            this.recordCodec = recordCodec;
            this.colMapping = colMapping;
            if (fragmentMetaInfo.hasValidEventTimeRange()) {
                this.filterTimeRangeChecker = new CompareFilterTimeRangeChecker(fragmentMetaInfo.getMinEventTime(),
                        fragmentMetaInfo.getMaxEventTime(), true);
            }
            buf = ByteBuffer.allocate(recordCodec.getMaxDimLength());
        }

        protected int mapCol(TblColRef col) {
            Integer i = colMapping.get(col);
            return i == null ? -1 : i;
        }

        @Override
        public TupleFilter transform(TupleFilter filter) {
            if (filter.getOperator() == TupleFilter.FilterOperatorEnum.NOT
                    && !TupleFilter.isEvaluableRecursively(filter)) {
                TupleFilter.collectColumns(filter, unEvaluableColumnCollector);
                return ConstantTupleFilter.TRUE;
            }

            // shortcut for unEvaluatable filter
            if (!filter.isEvaluable()) {
                TupleFilter.collectColumns(filter, unEvaluableColumnCollector);
                return ConstantTupleFilter.TRUE;
            }

            if (filter instanceof CompareTupleFilter) {
                return translateCompareFilter((CompareTupleFilter) filter);
            } else if (filter instanceof LogicalTupleFilter) {
                @SuppressWarnings("unchecked")
                ListIterator<TupleFilter> childIterator = (ListIterator<TupleFilter>) filter.getChildren().listIterator();
                while (childIterator.hasNext()) {
                    TupleFilter transformed = transform(childIterator.next());
                    if (transformed != null) {
                        childIterator.set(transformed);
                    } else {
                        throw new IllegalStateException("Should not be null");
                    }
                }
            }
            return filter;
        }


        @SuppressWarnings({ "rawtypes", "unchecked" })
        protected TupleFilter translateCompareFilter(CompareTupleFilter oldCompareFilter) {
            // extract ColumnFilter & ConstantFilter
            TblColRef externalCol = oldCompareFilter.getColumn();

            if (externalCol == null) {
                return oldCompareFilter;
            }

            Collection constValues = oldCompareFilter.getValues();
            if (constValues == null || constValues.isEmpty()) {
                return oldCompareFilter;
            }

            if (TimeDerivedColumnType.isTimeDerivedColumn(externalCol.getName()) && filterTimeRangeChecker != null) {
                CheckResult checkResult = filterTimeRangeChecker.check(oldCompareFilter,
                        TimeDerivedColumnType.getTimeDerivedColumnType(externalCol.getName()), timezoneOffset);
                if (checkResult == CheckResult.INCLUDED) {
                    return ConstantTupleFilter.TRUE;
                } else if (checkResult == CheckResult.EXCLUDED) {
                    return ConstantTupleFilter.FALSE;
                }
            }

            //CompareTupleFilter containing BuiltInFunctionTupleFilter will not reach here caz it will be transformed by BuiltInFunctionTransformer
            CompareTupleFilter newCompareFilter = new CompareTupleFilter(oldCompareFilter.getOperator());
            newCompareFilter.addChild(new ColumnTupleFilter(externalCol));

            //for CompareTupleFilter containing dynamicVariables, the below codes will actually replace dynamicVariables
            //with normal ConstantTupleFilter

            Object firstValue = constValues.iterator().next();
            int col = mapCol(externalCol);

            TupleFilter result;
            ByteArray code;

            // translate constant into code
            switch (newCompareFilter.getOperator()) {
            case EQ:
            case IN:
                Set newValues = Sets.newHashSet();
                for (Object value : constValues) {
                    code = translate(col, value, 0);
                    if (code != null)
                        newValues.add(code);
                }
                if (newValues.isEmpty()) {
                    result = ConstantTupleFilter.FALSE;
                } else {
                    newCompareFilter.addChild(new ConstantTupleFilter(newValues));
                    result = newCompareFilter;
                }
                break;
            case NOTIN:
                Set notInValues = Sets.newHashSet();
                for (Object value : constValues) {
                    code = translate(col, value, 0);
                    if (code != null)
                        notInValues.add(code);
                }
                if (notInValues.isEmpty()) {
                    result = ConstantTupleFilter.TRUE;
                } else {
                    newCompareFilter.addChild(new ConstantTupleFilter(notInValues));
                    result = newCompareFilter;
                }
                break;
            case NEQ:
                code = translate(col, firstValue, 0);
                if (code == null) {
                    result = ConstantTupleFilter.TRUE;
                } else {
                    newCompareFilter.addChild(new ConstantTupleFilter(code));
                    result = newCompareFilter;
                }
                break;
            case LT:
                code = translate(col, firstValue, 0);
                if (code == null) {
                    code = translate(col, firstValue, -1);
                    if (code == null)
                        result = ConstantTupleFilter.FALSE;
                    else
                        result = newCompareFilter(FilterOperatorEnum.LTE, externalCol, code);
                } else {
                    newCompareFilter.addChild(new ConstantTupleFilter(code));
                    result = newCompareFilter;
                }
                break;
            case LTE:
                code = translate(col, firstValue, -1);
                if (code == null) {
                    result = ConstantTupleFilter.FALSE;
                } else {
                    newCompareFilter.addChild(new ConstantTupleFilter(code));
                    result = newCompareFilter;
                }
                break;
            case GT:
                code = translate(col, firstValue, 0);
                if (code == null) {
                    code = translate(col, firstValue, 1);
                    if (code == null)
                        result = ConstantTupleFilter.FALSE;
                    else
                        result = newCompareFilter(FilterOperatorEnum.GTE, externalCol, code);
                } else {
                    newCompareFilter.addChild(new ConstantTupleFilter(code));
                    result = newCompareFilter;
                }
                break;
            case GTE:
                code = translate(col, firstValue, 1);
                if (code == null) {
                    result = ConstantTupleFilter.FALSE;
                } else {
                    newCompareFilter.addChild(new ConstantTupleFilter(code));
                    result = newCompareFilter;
                }
                break;
            default:
                throw new IllegalStateException("Cannot handle operator " + newCompareFilter.getOperator());
            }
            return result;
        }

        private TupleFilter newCompareFilter(FilterOperatorEnum op, TblColRef col, ByteArray code) {
            CompareTupleFilter r = new CompareTupleFilter(op);
            r.addChild(new ColumnTupleFilter(col));
            r.addChild(new ConstantTupleFilter(code));
            return r;
        }

        protected ByteArray translate(int col, Object value, int roundingFlag) {
            try {
                buf.clear();
                recordCodec.encodeDimension(col, value, roundingFlag, buf);
                int length = buf.position();
                return ByteArray.copyOf(buf.array(), 0, length);
            } catch (IllegalArgumentException ex) {
                return null;
            }
        }

        public void setTimezoneOffset(long timezoneOffset) {
            this.timezoneOffset = timezoneOffset;
        }
    }
}