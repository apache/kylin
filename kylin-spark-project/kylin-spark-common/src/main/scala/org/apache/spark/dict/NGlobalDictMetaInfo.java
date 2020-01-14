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
package org.apache.spark.dict;

import java.io.Serializable;

public class NGlobalDictMetaInfo implements Serializable {

    private int bucketSize;
    private long dictCount;
    private long[] bucketOffsets;
    private long[] bucketCount;

    NGlobalDictMetaInfo(int bucketSize, long[] bucketOffsets, long dictCount, long[] bucketCount) {
        this.bucketSize = bucketSize;
        this.dictCount = dictCount;
        this.bucketOffsets = bucketOffsets;
        this.bucketCount = bucketCount;
    }

    long getOffset(int point) {
        return bucketOffsets[point];
    }

    public int getBucketSize() {
        return bucketSize;
    }

    public void setBucketSize(int bucketSize) {
        this.bucketSize = bucketSize;
    }

    public long getDictCount() {
        return this.dictCount;
    }

    public long[] getBucketOffsets() {
        return this.bucketOffsets;
    }

    public long[] getBucketCount() {
        return this.bucketCount;
    }

    public void setDictCount(long dictCount) {
        this.dictCount = dictCount;
    }

    public void setBucketOffsets(long[] bucketOffsets) {
        this.bucketOffsets = bucketOffsets;
    }

    public void setBucketCount(long[] bucketCount) {
        this.bucketCount = bucketCount;
    }

    public boolean equals(final Object o) {
        if (o == this) return true;
        if (!(o instanceof NGlobalDictMetaInfo)) return false;
        final NGlobalDictMetaInfo other = (NGlobalDictMetaInfo) o;
        if (!other.canEqual((Object) this)) return false;
        if (this.getBucketSize() != other.getBucketSize()) return false;
        if (this.getDictCount() != other.getDictCount()) return false;
        if (!java.util.Arrays.equals(this.getBucketOffsets(), other.getBucketOffsets())) return false;
        if (!java.util.Arrays.equals(this.getBucketCount(), other.getBucketCount())) return false;
        return true;
    }

    protected boolean canEqual(final Object other) {
        return other instanceof NGlobalDictMetaInfo;
    }

    public int hashCode() {
        final int PRIME = 59;
        int result = 1;
        result = result * PRIME + this.getBucketSize();
        final long $dictCount = this.getDictCount();
        result = result * PRIME + (int) ($dictCount >>> 32 ^ $dictCount);
        result = result * PRIME + java.util.Arrays.hashCode(this.getBucketOffsets());
        result = result * PRIME + java.util.Arrays.hashCode(this.getBucketCount());
        return result;
    }
}