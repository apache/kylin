package org.apache.kylin.measure.auc;


import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import smile.validation.AUC;

import java.io.Serializable;
import java.util.List;

public class AucCounter implements Serializable {
    private List<Integer> truth = Lists.newLinkedList();
    private List<Double> pred = Lists.newLinkedList();

    public AucCounter() {
    }

    public AucCounter(AucCounter another) {
        merge(another);
    }

    public AucCounter(List<Integer> truth, List<Double> pred) {
        this.truth = truth == null ? Lists.newLinkedList() : truth;
        this.pred = pred == null ? Lists.newLinkedList() : pred;
    }


    public void merge(AucCounter value) {

        if (value == null) {
            return;
        }

        if (CollectionUtils.isEmpty(value.getTruth()) || CollectionUtils.isEmpty(value.getPred())) {
            return;
        }

        this.getTruth().addAll(value.getTruth());
        this.getPred().addAll(value.getPred());
    }

    public List<Integer> getTruth() {
        return truth;
    }

    public List<Double> getPred() {
        return pred;
    }

    public double auc() {
        if (CollectionUtils.isEmpty(truth) || CollectionUtils.isEmpty(pred)) {
            return -1;
        }

        int[] t = truth.stream().mapToInt(Integer::valueOf).toArray();
        double[] p = pred.stream().mapToDouble(Double::valueOf).toArray();
        double result = AUC.measure(t, p);
        if (Double.isNaN(result)) {
            return -1;
        }
        return result;
    }

    public void addTruth(Object t) {

        if (t == null) {
            throw new RuntimeException("Truth of dimension is null ");
        }
        truth.add((Integer) t);
    }

    public void addPred(Object p) {
        if (p == null) {
            throw new RuntimeException("Pred of dimension is null ");
        }
        pred.add((Double) p);
    }
}
