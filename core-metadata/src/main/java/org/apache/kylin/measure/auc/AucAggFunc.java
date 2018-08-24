package org.apache.kylin.measure.auc;

import org.apache.kylin.measure.ParamAsMeasureCount;

public class AucAggFunc implements ParamAsMeasureCount {
    @Override
    public int getParamAsMeasureCount() {
        return 2;
    }
}
