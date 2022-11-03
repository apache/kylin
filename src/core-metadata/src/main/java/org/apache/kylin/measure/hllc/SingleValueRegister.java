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

/**
 * Created by xiefan on 16-12-20.
 */
public class SingleValueRegister implements Register, java.io.Serializable {

    private int singleValuePos;

    private byte value;

    public SingleValueRegister() {
        this.singleValuePos = -1;
    }

    @Override
    public void set(int pos, byte value) {
        assert this.singleValuePos < 0 || this.singleValuePos == pos;
        this.singleValuePos = pos;
        this.value = value;
    }

    @Override
    public byte get(int pos) {
        if (pos != this.singleValuePos)
            return 0;
        return value;
    }

    @Override
    public void merge(Register another) {
        throw new IllegalStateException();
    }

    @Override
    public void clear() {
        this.singleValuePos = -1;
    }

    @Override
    public int getSize() {
        if (this.singleValuePos >= 0)
            return 1;
        return 0;
    }

    @Override
    public RegisterType getRegisterType() {
        return RegisterType.SINGLE_VALUE;
    }

    public int getSingleValuePos() {
        return singleValuePos;
    }

    public byte getValue() {
        return value;
    }

    public SparseRegister toSparse() {
        SparseRegister sr = new SparseRegister();
        if (singleValuePos >= 0)
            sr.set(singleValuePos, value);
        return sr;
    }

    public DenseRegister toDense(int p) {
        DenseRegister dr = new DenseRegister(p);
        if (singleValuePos >= 0) {
            dr.set(singleValuePos, value);
        }
        return dr;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + singleValuePos;
        result = prime * result + value;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        SingleValueRegister other = (SingleValueRegister) obj;
        if (this.singleValuePos != other.singleValuePos || this.value != other.value) {
            return false;
        }
        return true;
    }
}
