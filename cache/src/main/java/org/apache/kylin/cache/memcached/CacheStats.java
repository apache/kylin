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

package org.apache.kylin.cache.memcached;

public class CacheStats {
    private final long numHits;
    private final long numMisses;
    private final long getBytes;
    private final long getTime;
    private final long numPut;
    private final long putBytes;
    private final long numEvictions;
    private final long numTimeouts;
    private final long numErrors;

    public CacheStats(long getBytes, long getTime, long numPut, long putBytes, long numHits, long numMisses,
            long numEvictions, long numTimeouts, long numErrors) {
        this.getBytes = getBytes;
        this.getTime = getTime;
        this.numPut = numPut;
        this.putBytes = putBytes;
        this.numHits = numHits;
        this.numMisses = numMisses;
        this.numEvictions = numEvictions;
        this.numTimeouts = numTimeouts;
        this.numErrors = numErrors;
    }

    public long getNumHits() {
        return numHits;
    }

    public long getNumMisses() {
        return numMisses;
    }

    public long getNumGet() {
        return numHits + numMisses;
    }

    public long getNumGetBytes() {
        return getBytes;
    }

    public long getNumPutBytes() {
        return putBytes;
    }

    public long getNumPut() {
        return numPut;
    }

    public long getNumEvictions() {
        return numEvictions;
    }

    public long getNumTimeouts() {
        return numTimeouts;
    }

    public long getNumErrors() {
        return numErrors;
    }

    public long numLookups() {
        return numHits + numMisses;
    }

    public double hitRate() {
        long lookups = numLookups();
        return lookups == 0 ? 0 : numHits / (double) lookups;
    }

    public long avgGetBytes() {
        return getBytes == 0 ? 0 : getBytes / numLookups();
    }

    public long getAvgGetTime() {
        return getTime / numLookups();
    }
}