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

package org.apache.kylin.common.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @author ysong1
 * 
 */
public class RandomSampler<T> {

    private Random rdm = new Random();

    public List<T> sample(List<T> data, int sampleNumber) {
        if (data == null) {
            throw new IllegalArgumentException("Input list is null");
        }
        if (data.size() < sampleNumber) {
            return data;
        }

        List<T> result = new ArrayList<T>(sampleNumber);
        int n = data.size();
        for (int i = 0; i < n; i++) {
            if (i < sampleNumber) {
                result.add(data.get(i));
            } else {
                int j = rdm.nextInt(i);
                if (j < sampleNumber) {
                    result.set(j, data.get(i));
                }
            }
        }
        return result;
    }
}
