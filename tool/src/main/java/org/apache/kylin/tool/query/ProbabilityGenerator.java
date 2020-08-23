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

package org.apache.kylin.tool.query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProbabilityGenerator {

    private static final Logger logger = LoggerFactory.getLogger(ProbabilityGenerator.class);

    public static double[] generate(int size) {
        double[] probArray = generateProbabilityList(size);
        return generateProbabilityCumulative(probArray);
    }

    public static int searchIndex(double p, double[] pCumArray) {
        return binarySearchIndex(p, pCumArray, 0, pCumArray.length - 1);
    }

    private static int binarySearchIndex(double key, double[] array, int from, int to) {
        if (from < 0 || to < 0) {
            throw new IllegalArgumentException("params from & length must larger than 0 .");
        }
        if (key < array[from]) {
            return from - 1;
        } else if (key >= array[to]) {
            return to;
        }

        int middle = (from >>> 1) + (to >>> 1);
        double temp = array[middle];
        if (temp > key) {
            to = middle - 1;
        } else if (temp < key) {
            from = middle + 1;
        } else {
            return middle;
        }
        return binarySearchIndex(key, array, from, to);
    }

    public static double[] generateProbabilityCumulative(double[] pQueryArray) {
        double[] pCumArray = new double[pQueryArray.length];
        pCumArray[0] = 0;
        for (int i = 0; i < pQueryArray.length - 1; i++) {
            pCumArray[i + 1] = pCumArray[i] + pQueryArray[i];
        }
        return pCumArray;
    }

    public static double[] generateProbabilityList(int nOfEle) {
        Integer[] nHitArray = new Integer[nOfEle];
        double[] pQueryArray = new double[nOfEle];

        int sumHit = generateHitNumberList(nHitArray);

        if (sumHit == 0)
            throw new IllegalStateException();

        for (int i = 0; i < nOfEle; i++) {
            pQueryArray[i] = nHitArray[i] * 1.0 / sumHit;
        }
        return pQueryArray;
    }

    public static int generateHitNumberList(Integer[] nHitArray) {
        int sumHit = 0;
        for (int i = 0; i < nHitArray.length; i++) {
            int randomNum = 1 + (int) (Math.random() * nHitArray.length);
            nHitArray[i] = randomNum * randomNum;
            sumHit += nHitArray[i];
        }
        return sumHit;
    }
}
