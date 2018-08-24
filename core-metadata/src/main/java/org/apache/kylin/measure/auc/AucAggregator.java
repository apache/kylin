package org.apache.kylin.measure.auc;

import org.apache.kylin.measure.MeasureAggregator;

public class AucAggregator extends MeasureAggregator<AucCounter> {
    AucCounter auc = null;

    public AucAggregator() {
    }

    @Override
    public void reset() {
        auc = null;
    }

    @Override
    public void aggregate(AucCounter value) {
        if (auc == null)
            auc = new AucCounter(value);
        else
            auc.merge(value);
    }

    @Override
    public AucCounter aggregate(AucCounter value1, AucCounter value2) {
        if (value1 == null) {
            return value2;
        } else if (value2 == null) {
            return value1;
        }
        value1.merge(value2);
        return value1;
    }

    @Override
    public AucCounter getState() {
        return auc;
    }

    @Override
    public int getMemBytesEstimate() {
        return 0;
    }
}
