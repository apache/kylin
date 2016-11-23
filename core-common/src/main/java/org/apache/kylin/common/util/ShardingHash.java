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

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public class ShardingHash {

    static HashFunction hashFunc = Hashing.murmur3_128();

    public static short getShard(int integerValue, int totalShards) {
        if (totalShards <= 1) {
            return 0;
        }
        long hash = hashFunc.hashInt(integerValue).asLong();
        return _getShard(hash, totalShards);
    }

    public static short getShard(long longValue, int totalShards) {
        if (totalShards <= 1) {
            return 0;
        }
        long hash = hashFunc.hashLong(longValue).asLong();
        return _getShard(hash, totalShards);
    }

    public static short getShard(byte[] byteValues, int offset, int length, int totalShards) {
        if (totalShards <= 1) {
            return 0;
        }

        long hash = hashFunc.hashBytes(byteValues, offset, length).asLong();
        return _getShard(hash, totalShards);
    }

    public static short normalize(short cuboidShardBase, short shardOffset, int totalShards) {
        if (totalShards <= 1) {
            return 0;
        }
        return (short) ((cuboidShardBase + shardOffset) % totalShards);
    }

    private static short _getShard(long hash, int totalShard) {
        long x = hash % totalShard;
        if (x < 0) {
            x += totalShard;
        }
        return (short) x;
    }
}
