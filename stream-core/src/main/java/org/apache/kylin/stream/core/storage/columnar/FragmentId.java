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

package org.apache.kylin.stream.core.storage.columnar;

public class FragmentId implements Comparable<FragmentId> {
    private static final String SEP = "-";
    private int startId;
    private int endId;

    public static FragmentId parse(String idString) {
        String[] splits = idString.split(SEP);
        if (splits.length == 1) {
            return new FragmentId(Integer.parseInt(splits[0]));
        } else if (splits.length == 2) {
            return new FragmentId(Integer.parseInt(splits[0]), Integer.parseInt(splits[1]));
        } else {
            throw new IllegalArgumentException("illegal fragment id format:" + idString);
        }
    }

    public FragmentId(int id) {
        this.startId = id;
        this.endId = id;
    }

    public FragmentId(int startId, int endId) {
        this.startId = startId;
        this.endId = endId;
    }

    public int getStartId() {
        return startId;
    }

    public int getEndId() {
        return endId;
    }

    @Override
    public int compareTo(FragmentId other) {
        return endId - other.endId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        FragmentId that = (FragmentId) o;

        if (startId != that.startId)
            return false;
        return endId == that.endId;

    }

    @Override
    public int hashCode() {
        int result = startId;
        result = 31 * result + endId;
        return result;
    }

    @Override
    public String toString() {
        if (startId == endId) {
            return String.valueOf(endId);
        }
        return startId + SEP + endId;
    }
}
