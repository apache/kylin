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

package org.apache.kylin.storage.druid;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.druid.java.util.common.Intervals;
import io.druid.query.ordering.StringComparator;
import io.druid.query.ordering.StringComparators;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
//import org.apache.kylin.measure.bitmap.BitmapMeasureType;
//import org.apache.kylin.measure.extendedcolumn.ExtendedColumnMeasureType;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import io.druid.data.input.impl.DimensionSchema;
import io.druid.data.input.impl.StringDimensionSchema;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.DoubleMaxAggregatorFactory;
import io.druid.query.aggregation.DoubleMinAggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.aggregation.FloatMaxAggregatorFactory;
import io.druid.query.aggregation.FloatMinAggregatorFactory;
import io.druid.query.aggregation.FloatSumAggregatorFactory;
import io.druid.query.aggregation.LongMaxAggregatorFactory;
import io.druid.query.aggregation.LongMinAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
//import io.druid.query.aggregation.decimal.DecimalMaxAggregatorFactory;
//import io.druid.query.aggregation.decimal.DecimalMinAggregatorFactory;
//import io.druid.query.aggregation.decimal.DecimalSumAggregatorFactory;
//import io.druid.query.aggregation.kylin.distinctcount.DistinctCountAggregatorFactory;
//import io.druid.query.aggregation.kylin.extendcolumn.ExtendColumnAggregatorFactory;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;

public class DruidSchema implements NameMapping {
    public static final String ID_COL = "_CUBOID_ID_";

    public static final Interval ETERNITY_INTERVAL = new Interval(
            new DateTime("0000-01-01", DateTimeZone.UTC),
            new DateTime("3000-01-01", DateTimeZone.UTC)
    );

    private final NameMapping mapping;
    private final List<TblColRef> dimensions;
    private final List<MeasureDesc> measures;

    // computed
    private Map<String, Integer> indexByName = new HashMap<>();

    public DruidSchema(NameMapping mapping, List<TblColRef> dimensions, List<MeasureDesc> measures) {
        this.mapping = mapping;
        this.dimensions = ImmutableList.copyOf(dimensions);
        this.measures = ImmutableList.copyOf(measures);

        int fieldIndex = 0;
        for (TblColRef dim : dimensions) {
            String fieldName = getDimFieldName(dim);
            indexByName.put(fieldName, fieldIndex++);
        }
        for (MeasureDesc measure : measures) {
            String fieldName = getMeasureFieldName(measure);
            indexByName.put(fieldName, fieldIndex++);
        }
    }

    public static Interval segmentInterval(CubeSegment segment) {
        if (!segment.getCubeDesc().getModel().getPartitionDesc().isPartitioned()) {
            return Intervals.utc(0, segment.getCreateTimeUTC());
        }
        return Intervals.utc(segment.getTSRange().start.v, segment.getTSRange().end.v);
    }

    public static String getDataSource(CubeDesc cubeDesc) {
        return cubeDesc.getName() + "_" + cubeDesc.getOnlineVersion();
    }

    public static StringComparator dimensionComparator(DataType dataType) {
        if (dataType.isStringFamily()) {
            return StringComparators.LEXICOGRAPHIC;
        }
        return StringComparators.NUMERIC;
    }

    @Override
    public String getDimFieldName(TblColRef dim) {
        return mapping.getDimFieldName(dim);
    }

    @Override
    public String getMeasureFieldName(MeasureDesc measure) {
        return mapping.getMeasureFieldName(measure);
    }

    public List<TblColRef> getDimensions() {
        return dimensions;
    }

    public List<MeasureDesc> getMeasures() {
        return measures;
    }

    public int getTotalFieldCount() {
        return dimensions.size() + measures.size();
    }

    public int getFieldIndex(String field) {
        return indexByName.get(field);
    }

    public List<DimensionSchema> getDimensionSchemas() {
        List<DimensionSchema> result = new ArrayList<>();
        result.add(new StringDimensionSchema(ID_COL));
        for (TblColRef dim : dimensions) {
            result.add(new StringDimensionSchema(getDimFieldName(dim)));
        }
        return result;
    }

    public List<DimensionSpec> getQueryDimensionSpec(Collection<TblColRef> dimensions) {
        List<DimensionSpec> result = Lists.newArrayListWithCapacity(dimensions.size());
        for (TblColRef dim : dimensions) {
            String name = getDimFieldName(dim);
            result.add(new DefaultDimensionSpec(name, name));
        }
        return result;
    }

    public AggregatorFactory[] getAggregators() {
        return getAggregators(measures);
    }

    public AggregatorFactory[] getAggregators(List<MeasureDesc> measures) {
        Iterable<AggregatorFactory> aggregatorFactories = Iterables.transform(measures, new Function<MeasureDesc, AggregatorFactory>() {
            @Override
            public AggregatorFactory apply(MeasureDesc measure) {
                String name = getMeasureFieldName(measure);
                FunctionDesc func = measure.getFunction();
                DataType returnType = func.getReturnDataType();
                switch (func.getExpression()) {
                case FunctionDesc.FUNC_COUNT:
                    return new LongSumAggregatorFactory(name, name);
                case FunctionDesc.FUNC_SUM: {
                    if (returnType.isIntegerFamily()) {
                        return new LongSumAggregatorFactory(name, name);
                    }
                    if (returnType.isFloat()) {
                        return new FloatSumAggregatorFactory(name, name);
                    }
                    if (returnType.isDouble()) {
                        return new DoubleSumAggregatorFactory(name, name);
                    }
//                    if (returnType.isDecimal()) {
//                        return new DecimalSumAggregatorFactory(name, name, returnType.getPrecision());
//                    }
                    break;
                }
                case FunctionDesc.FUNC_MIN: {
                    if (returnType.isIntegerFamily()) {
                        return new LongMinAggregatorFactory(name, name);
                    }
                    if (returnType.isFloat()) {
                        return new FloatMinAggregatorFactory(name, name);
                    }
                    if (returnType.isDouble()) {
                        return new DoubleMinAggregatorFactory(name, name);
                    }
//                    if (returnType.isDecimal()) {
//                        return new DecimalMinAggregatorFactory(name, name, returnType.getPrecision());
//                    }
                    break;
                }
                case FunctionDesc.FUNC_MAX: {
                    if (returnType.isIntegerFamily()) {
                        return new LongMaxAggregatorFactory(name, name);
                    }
                    if (returnType.isFloat()) {
                        return new FloatMaxAggregatorFactory(name, name);
                    }
                    if (returnType.isDouble()) {
                        return new DoubleMaxAggregatorFactory(name, name);
                    }
//                    if (returnType.isDecimal()) {
//                        return new DecimalMaxAggregatorFactory(name, name, returnType.getPrecision());
//                    }
                    break;
                }
//                case FunctionDesc.FUNC_COUNT_DISTINCT:
//                    if (returnType.getName().equals(BitmapMeasureType.DATATYPE_BITMAP)) {
//                        return new DistinctCountAggregatorFactory(name, name);
//                    }
//                    break;
//                case ExtendedColumnMeasureType.FUNC_EXTENDED_COLUMN:
//                    return new ExtendColumnAggregatorFactory(name, name, returnType.getPrecision());
                    // TODO support hll
                default:
                    throw new AssertionError(func.getExpression());
                }
                throw new UnsupportedOperationException(func + " is not supported");

            }
        });
        return Iterables.toArray(aggregatorFactories, AggregatorFactory.class);
    }
}
