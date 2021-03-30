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

package org.apache.kylin.rest.signature;

class SegmentSignature extends ComponentSignature<SegmentSignature> {
    public final String name;
    public final long lastBuildTime;

    public SegmentSignature(String name, long lastBuildTime) {
        this.name = name;
        this.lastBuildTime = lastBuildTime;
    }

    public String getKey() {
        return name;
    }

    @Override
    public int compareTo(SegmentSignature o) {
        return name.compareTo(o.name);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        SegmentSignature that = (SegmentSignature) o;

        if (lastBuildTime != that.lastBuildTime)
            return false;
        return name != null ? name.equals(that.name) : that.name == null;

    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (int) (lastBuildTime ^ (lastBuildTime >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return name + ":" + lastBuildTime;
    }
}
