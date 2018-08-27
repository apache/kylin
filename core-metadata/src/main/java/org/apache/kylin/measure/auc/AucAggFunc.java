package org.apache.kylin.measure.auc;

import org.apache.kylin.measure.ParamAsMeasureCount;

public class AucAggFunc implements ParamAsMeasureCount {

    public static AucCounter init() {
        return null;
    }

    public static AucCounter add(AucCounter counter, Object t, Object p) {
        if (counter == null) {
            counter = new AucCounter();
        }
        counter.addTruth(t);
        counter.addPred(p);
        return counter;
    }

    public static AucCounter merge(AucCounter counter0, AucCounter counter1) {
        counter0.merge(counter1);
        return counter0;
    }

    public static double result(AucCounter counter) {
        return counter == null ? -1D : counter.auc();
    }

    @Override
    public int getParamAsMeasureCount() {
        return 2;
    }
}
