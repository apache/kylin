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

    public void merge(AucCounter value) {

        if (value == null) {
            return;
        }
        if (CollectionUtils.isEmpty(value.getTruth())) {
            return;
        }
        if (CollectionUtils.isEmpty(value.getPred())) {
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

        int[] t = truth.stream().mapToInt(Integer::valueOf).toArray();
        double[] p = pred.stream().mapToDouble(Double::valueOf).toArray();

        return AUC.measure(t, p);
    }
}
