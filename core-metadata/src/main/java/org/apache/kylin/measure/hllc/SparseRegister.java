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
package org.apache.kylin.measure.hllc;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by xiefan on 16-12-9.
 */
public class SparseRegister implements Register {

    private int overThreshold;

    private Map<Integer, Byte> sparseRegister = new TreeMap<>();

    public SparseRegister(int overThreshold) {
        this.overThreshold = overThreshold;
    }

    public DenseRegister toDense(int p) {
        DenseRegister dr = new DenseRegister(p);
        for (Map.Entry<Integer, Byte> entry : sparseRegister.entrySet()) {
            dr.set(entry.getKey(), entry.getValue());
        }
        return dr;
    }

    @Override
    public void set(int pos, byte value) {
        sparseRegister.put(pos, value);
    }

    @Override
    public Byte get(int pos) {
        return sparseRegister.get(pos);
    }

    @Override
    public void merge(Register another) {
        assert another instanceof SparseRegister;
        SparseRegister sr = (SparseRegister) another;
        for (Map.Entry<Integer, Byte> entry : sr.sparseRegister.entrySet()) {
            Byte v = sparseRegister.get(entry.getKey());
            if (v == null || entry.getValue() > v)
                sparseRegister.put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void clear() {
        sparseRegister.clear();
    }

    @Override
    public int getSize() {
        return sparseRegister.size();
    }

    @Override
    public int getHashCode() {
        final int prime = 31;
        int result = 1;
        for (Map.Entry<Integer, Byte> entry : sparseRegister.entrySet()) {
            result = prime * result + entry.getKey();
            result = prime * result + entry.getValue();
        }
        return result;
    }

    public boolean isOverThreshold() {
        if (this.sparseRegister.size() > overThreshold)
            return true;
        return false;
    }

    public Collection<Map.Entry<Integer, Byte>> getAllValue() {
        return Collections.unmodifiableCollection(sparseRegister.entrySet());
    }

}
