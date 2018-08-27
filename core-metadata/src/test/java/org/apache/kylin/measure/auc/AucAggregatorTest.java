package org.apache.kylin.measure.auc;

import com.google.common.collect.Lists;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.junit.Test;
import smile.validation.AUC;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class AucAggregatorTest {
    @Test
    public void testAggregate() {
        int datasize = 10000;
        AucAggregator aggregator = new AucAggregator();
        RandomDataGenerator randomData = new RandomDataGenerator();
        List<Integer> truths = Lists.newArrayListWithCapacity(datasize);
        List<Double> preds = Lists.newArrayListWithCapacity(datasize);
        for (int i = 0; i < datasize; i++) {
            int t = randomData.nextInt(0, 1);
            double p = Math.random();
            truths.add(t);
            preds.add(p);

            AucCounter c = new AucCounter();
            c.addTruth(t);
            c.addPred(p);
            aggregator.aggregate(c);
        }

        double actualResult = aggregator.getState().auc();
        double expectResult = auc(truths, preds);
        assertEquals(expectResult, actualResult, 0.001);
    }


    public double auc(List<Integer> truth, List<Double> pred) {

        int[] t = truth.stream().mapToInt(Integer::valueOf).toArray();
        double[] p = pred.stream().mapToDouble(Double::valueOf).toArray();
        double result = AUC.measure(t, p);
        if (Double.isNaN(result)) {
            return -1;
        }
        return result;
    }
}
