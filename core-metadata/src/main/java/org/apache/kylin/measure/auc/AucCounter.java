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
