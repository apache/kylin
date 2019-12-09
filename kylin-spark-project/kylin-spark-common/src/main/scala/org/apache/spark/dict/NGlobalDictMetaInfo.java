/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
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