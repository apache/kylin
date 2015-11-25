package org.apache.kylin.aggregation;

public interface IAggregationFactory {

    public AggregationType createAggregationType(String funcName, String dataType);
}
