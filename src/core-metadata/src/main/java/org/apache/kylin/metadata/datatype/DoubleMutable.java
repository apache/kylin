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

package org.apache.kylin.metadata.datatype;

import java.io.Serializable;

@SuppressWarnings("serial")
public class DoubleMutable implements Comparable<DoubleMutable>, Serializable {

    private double v;

    public DoubleMutable() {
        this(0);
    }

    public DoubleMutable(double v) {
        set(v);
    }

    public double get() {
        return v;
    }

    public void set(double v) {
        this.v = v;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof DoubleMutable)) {
            return false;
        }
        DoubleMutable other = (DoubleMutable) o;
        return this.v == other.v;
    }

    @Override
    public int hashCode() {
        return (int) Double.doubleToLongBits(v);
    }

    @Override
    public int compareTo(DoubleMutable o) {
        return (v < o.v ? -1 : (v == o.v ? 0 : 1));
    }

    @Override
    public String toString() {
        return Double.toString(v);
    }

}
