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

package org.apache.kylin.rest.cache.memcached;

public class CacheStats {
    private final long getBytes;
    private final long getTime;
    private final long putBytes;
    private final CacheStatsCounter cacheStatsCounter;

    public CacheStats(long getBytes, long getTime, long putBytes, CacheStatsCounter cacheStatsCounter) {
        this.getBytes = getBytes;
        this.getTime = getTime;
        this.putBytes = putBytes;
        this.cacheStatsCounter = cacheStatsCounter;
    }

    static class CacheStatsCounter {
        final long numHits;
        final long numMisses;
        final long numPut;
        final long numEvictions;
        final long numTimeouts;
        final long numErrors;

        CacheStatsCounter(long numPut, long numHits, long numMisses, long numEvictions, long numTimeouts,
                long numErrors) {
            this.numPut = numPut;
            this.numHits = numHits;
            this.numMisses = numMisses;
            this.numEvictions = numEvictions;
            this.numTimeouts = numTimeouts;
            this.numErrors = numErrors;
        }
    }

    public long getNumHits() {
        return cacheStatsCounter.numHits;
    }

    public long getNumMisses() {
        return cacheStatsCounter.numMisses;
    }

    public long getNumGet() {
        return cacheStatsCounter.numHits + cacheStatsCounter.numMisses;
    }

    public long getNumGetBytes() {
        return getBytes;
    }

    public long getNumPutBytes() {
        return putBytes;
    }

    public long getNumPut() {
        return cacheStatsCounter.numPut;
    }

    public long getNumEvictions() {
        return cacheStatsCounter.numEvictions;
    }

    public long getNumTimeouts() {
        return cacheStatsCounter.numTimeouts;
    }

    public long getNumErrors() {
        return cacheStatsCounter.numErrors;
    }

    public long numLookups() {
        return cacheStatsCounter.numHits + cacheStatsCounter.numMisses;
    }

    public double hitRate() {
        long lookups = numLookups();
        return lookups == 0 ? 0 : cacheStatsCounter.numHits / (double) lookups;
    }

    public long avgGetBytes() {
        return getBytes == 0 ? 0 : getBytes / numLookups();
    }

    public long getAvgGetTime() {
        return getTime / numLookups();
    }
}
