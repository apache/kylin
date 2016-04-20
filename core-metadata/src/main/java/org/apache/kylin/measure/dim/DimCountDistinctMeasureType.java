package org.apache.kylin.measure.dim;

import org.apache.kylin.measure.MeasureAggregator;
import org.apache.kylin.measure.MeasureIngester;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.measure.MeasureTypeFactory;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.realization.SQLDigest;

/**
 * Created by dongli on 4/20/16.
 */
public class DimCountDistinctMeasureType extends MeasureType<Object> {
    public static class DimCountDistinctMeasureTypeFactory extends MeasureTypeFactory<Object> {

        @Override
        public MeasureType<Object> createMeasureType(String funcName, DataType dataType) {
            return new DimCountDistinctMeasureType();
        }

        @Override
        public String getAggrFunctionName() {
            return null;
        }

        @Override
        public String getAggrDataTypeName() {
            return null;
        }

        @Override
        public Class getAggrDataTypeSerializer() {
            return null;
        }

    }
    @Override
    public MeasureIngester newIngester() {
        throw new UnsupportedOperationException("No ingester for this measure type.");
    }

    @Override
    public MeasureAggregator newAggregator() {
        throw new UnsupportedOperationException("No aggregator for this measure type.");
    }

    @Override
    public boolean needRewrite() {
        return true;
    }

    @Override
    public boolean needRewriteField() {
        return false;
    }

    @Override
    public Class<?> getRewriteCalciteAggrFunctionClass() {
        return DimCountDistinctAggFunc.class;
    }

    public void adjustSqlDigest(MeasureDesc measureDesc, SQLDigest sqlDigest) {
        sqlDigest.groupbyColumns.addAll(measureDesc.getFunction().getParameter().getColRefs());
        sqlDigest.aggregations.remove(measureDesc.getFunction());
    }
}
