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

package org.apache.kylin.measure.extendedcolumn;

import java.nio.charset.Charset;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.measure.MeasureAggregator;
import org.apache.kylin.measure.MeasureIngester;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.measure.MeasureTypeFactory;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.tuple.Tuple;
import org.apache.kylin.metadata.tuple.TupleInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.guava30.shaded.common.collect.Lists;

public class ExtendedColumnMeasureType extends MeasureType<ByteArray> {

    private static final Logger logger = LoggerFactory.getLogger(ExtendedColumnMeasureType.class);

    public static final String FUNC_EXTENDED_COLUMN = "EXTENDED_COLUMN";
    public static final String DATATYPE_EXTENDED_COLUMN = "extendedcolumn";
    private final DataType dataType;

    public static class Factory extends MeasureTypeFactory<ByteArray> {

        @Override
        public MeasureType<ByteArray> createMeasureType(String funcName, DataType dataType) {
            return new ExtendedColumnMeasureType(funcName, dataType);
        }

        @Override
        public String getAggrFunctionName() {
            return FUNC_EXTENDED_COLUMN;
        }

        @Override
        public String getAggrDataTypeName() {
            return DATATYPE_EXTENDED_COLUMN;
        }

        @Override
        public Class<? extends DataTypeSerializer<ByteArray>> getAggrDataTypeSerializer() {
            return ExtendedColumnSerializer.class;
        }
    }

    public ExtendedColumnMeasureType(String funcName, DataType dataType) {
        this.dataType = dataType;
    }

    public static List<TblColRef> getExtendedColumnHosts(FunctionDesc functionDesc) {
        List<TblColRef> ret = Lists.newArrayList();
        List<TblColRef> params = functionDesc.getColRefs();
        for (int i = 0; i < params.size() - 1; i++) {
            ret.add(params.get(i));
        }
        return ret;
    }

    public static TblColRef getExtendedColumn(FunctionDesc functionDesc) {
        List<TblColRef> params = functionDesc.getColRefs();
        return params.get(params.size() - 1);
    }

    public void adjustSqlDigest(List<MeasureDesc> measureDescs, SQLDigest sqlDigest) {
        for (MeasureDesc measureDesc : measureDescs) {
            if (!sqlDigest.involvedMeasure.contains(measureDesc)) {
                continue;
            }
            FunctionDesc extendColumnFunc = measureDesc.getFunction();
            List<TblColRef> hosts = getExtendedColumnHosts(extendColumnFunc);
            TblColRef extended = getExtendedColumn(extendColumnFunc);

            if (!sqlDigest.groupbyColumns.contains(extended)) {
                continue;
            }

            sqlDigest.aggregations.add(extendColumnFunc);
            sqlDigest.groupbyColumns.remove(extended);
            sqlDigest.groupbyColumns.addAll(hosts);
            sqlDigest.metricColumns.add(extended);
        }
    }

    @Override
    public CapabilityResult.CapabilityInfluence influenceCapabilityCheck(Collection<TblColRef> unmatchedDimensions,
            Collection<FunctionDesc> unmatchedAggregations, SQLDigest digest, final MeasureDesc measureDesc) {
        TblColRef extendedCol = getExtendedColumn(measureDesc.getFunction());

        if (!unmatchedDimensions.contains(extendedCol)) {
            return null;
        }

        if (digest.filterColumns.contains(extendedCol)) {
            return null;
        }

        unmatchedDimensions.remove(extendedCol);

        return new CapabilityResult.CapabilityInfluence() {
            @Override
            public double suggestCostMultiplier() {
                return 0.9;
            }

            @Override
            public MeasureDesc getInvolvedMeasure() {
                return measureDesc;
            }
        };
    }

    public boolean needAdvancedTupleFilling() {
        return true;
    }

    public IAdvMeasureFiller getAdvancedTupleFiller(FunctionDesc function, TupleInfo returnTupleInfo,
            Map<TblColRef, Dictionary<String>> dictionaryMap) {
        final TblColRef extended = getExtendedColumn(function);
        final int extendedColumnInTupleIdx = returnTupleInfo.hasColumn(extended)
                ? returnTupleInfo.getColumnIndex(extended)
                : -1;

        if (extendedColumnInTupleIdx == -1) {
            throw new RuntimeException("Extended column is not required in returnTupleInfo");
        }

        return new IAdvMeasureFiller() {
            private String value;

            @Override
            public void reload(Object measureValue) {
                if (measureValue == null) {
                    value = null;
                    return;
                }

                ByteArray byteArray = (ByteArray) measureValue;
                //the array in ByteArray is guaranteed to be completed owned by the ByteArray
                value = Bytes.toString(byteArray.array());
            }

            @Override
            public int getNumOfRows() {
                return 1;
            }

            @Override
            public void fillTuple(Tuple tuple, int row) {
                tuple.setDimensionValue(extendedColumnInTupleIdx, value);
            }
        };
    }

    @Override
    public MeasureIngester<ByteArray> newIngester() {

        return new MeasureIngester<ByteArray>() {

            public String truncateWhenUTF8(String s, int maxBytes) {
                int b = 0;
                for (int i = 0; i < s.length(); i++) {
                    char c = s.charAt(i);

                    // ranges from http://en.wikipedia.org/wiki/UTF-8
                    int skip = 0;
                    int more;
                    if (c <= 0x007f) {
                        more = 1;
                    } else if (c <= 0x07FF) {
                        more = 2;
                    } else if (c <= 0xd7ff) {
                        more = 3;
                    } else if (c <= 0xDFFF) {
                        // surrogate area, consume next char as well
                        more = 4;
                        skip = 1;
                    } else {
                        more = 3;
                    }

                    if (b + more > maxBytes) {
                        return s.substring(0, i);
                    }
                    b += more;
                    i += skip;
                }
                return s;
            }

            @Override
            public ByteArray valueOf(String[] values, MeasureDesc measureDesc,
                    Map<TblColRef, Dictionary<String>> dictionaryMap) {
                if (values.length <= 1)
                    throw new IllegalArgumentException();

                String literal = values[values.length - 1];
                if (literal == null) {
                    return new ByteArray();
                }

                byte[] bytes = Bytes.toBytes(literal);
                if (bytes.length <= dataType.getPrecision()) {
                    return new ByteArray(bytes);
                } else {
                    return new ByteArray(
                            truncateWhenUTF8(literal, dataType.getPrecision()).getBytes(Charset.defaultCharset()));
                }
            }
        };
    }

    @SuppressWarnings("serial")
    @Override
    public MeasureAggregator<ByteArray> newAggregator() {
        return new MeasureAggregator<ByteArray>() {
            private ByteArray byteArray = null;
            private boolean warned = false;

            @Override
            public void reset() {
                byteArray = null;
            }

            @Override
            public void aggregate(ByteArray value) {
                if (byteArray == null) {
                    byteArray = value;
                } else {
                    if (!byteArray.equals(value)) {
                        if (!warned) {
                            logger.warn("Extended column must be unique given same host column");
                            warned = true;
                        }
                    }
                }
            }

            @Override
            public ByteArray aggregate(ByteArray value1, ByteArray value2) {
                if (value1 == null) {
                    return value2;
                } else if (value2 == null) {
                    return value1;
                } else if (!value1.equals(value2)) {
                    if (!warned) {
                        logger.warn("Extended column must be unique given same host column");
                        warned = true;
                    }
                }
                return value1;
            }

            @Override
            public ByteArray getState() {
                return byteArray;
            }

            @Override
            public int getMemBytesEstimate() {
                return dataType.getPrecision() / 2;
            }
        };
    }

    @Override
    public boolean needRewrite() {
        return false;
    }
}
