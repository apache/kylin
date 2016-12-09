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

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

/**
 * Created by xiefan on 16-12-9.
 */
public class DenseRegister implements Register {
    private int p;

    private int m;

    private byte[] register;

    public DenseRegister(int p) {
        this.m = 1 << p;
        this.register = new byte[m];
    }

    public void set(int pos, byte value) {
        register[pos] = value;
    }

    @Override
    public Byte get(int pos) {
        return register[pos];
    }

    @Override
    public void merge(Register another) {
        if (another instanceof DenseRegister) {
            DenseRegister dr = (DenseRegister) another;
            for (int i = 0; i < register.length; i++) {
                if (dr.register[i] > register[i])
                    register[i] = dr.register[i];
            }
        } else {
            SparseRegister sr = (SparseRegister) another;
            Collection<Map.Entry<Integer, Byte>> allValue = sr.getAllValue();
            for (Map.Entry<Integer, Byte> entry : allValue) {
                if (entry.getValue() > register[entry.getKey()])
                    register[entry.getKey()] = entry.getValue();
            }
        }
    }

    @Override
    public void clear() {
        byte zero = (byte) 0;
        Arrays.fill(register, zero);
    }

    @Override
    public int getSize() {
        int size = 0;
        for (int i = 0; i < m; i++) {
            if (register[i] > 0)
                size++;
        }
        return size;
    }

    @Override
    public int getHashCode() {
        return Arrays.hashCode(register);
    }

    public byte[] getRawRegister() {
        return this.register;
    }

}
