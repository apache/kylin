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

/**
 * Utility class to manage a quadruple. Copied from org.apache.hadoop.hbase.util.Pair
 */
public class Quadruple<A, B, C, D> {
    private A first;
    private B second;
    private C third;
    private D fourth;

    public Quadruple(A first, B second, C third, D fourth) {
        this.first = first;
        this.second = second;
        this.third = third;
        this.fourth = fourth;
    }

    // ctor cannot infer types w/o warning but a method can.
    public static <A, B, C, D> Quadruple<A, B, C, D> create(A first, B second, C third, D fourth) {
        return new Quadruple<A, B, C, D>(first, second, third, fourth);
    }

    public int hashCode() {
        int hashFirst = (first != null ? first.hashCode() : 0);
        int hashSecond = (second != null ? second.hashCode() : 0);
        int hashThird = (third != null ? third.hashCode() : 0);
        int hashFourth = (fourth != null ? fourth.hashCode() : 0);

        return (hashFirst >> 1) ^ hashSecond ^ (hashThird << 1) ^ (hashFourth << 1);
    }

    public boolean equals(Object obj) {
        if (!(obj instanceof Quadruple)) {
            return false;
        }

        Quadruple<?, ?, ?, ?> otherQuadruple = (Quadruple<?, ?, ?, ?>) obj;

        if (first != otherQuadruple.first && (first != null && !(first.equals(otherQuadruple.first))))
            return false;
        if (second != otherQuadruple.second && (second != null && !(second.equals(otherQuadruple.second))))
            return false;
        if (third != otherQuadruple.third && (third != null && !(third.equals(otherQuadruple.third))))
            return false;
        if (fourth != otherQuadruple.fourth && (fourth != null && !(fourth.equals(otherQuadruple.fourth))))
            return false;

        return true;
    }

    public String toString() {
        return "(" + first + ", " + second + ", " + third + ", " + fourth + ")";
    }

    public A getFirst() {
        return first;
    }

    public void setFirst(A first) {
        this.first = first;
    }

    public B getSecond() {
        return second;
    }

    public void setSecond(B second) {
        this.second = second;
    }

    public C getThird() {
        return third;
    }

    public void setThird(C third) {
        this.third = third;
    }

    public D getFourth() {
        return fourth;
    }

    public void setFourth(D fourth) {
        this.fourth = fourth;
    }
}



