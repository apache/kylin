package com.kylinolap.cube.invertedindex;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public class ShardingHash {

    static HashFunction hashFunc = Hashing.murmur3_128();
    
    public static long hashInt(int integer) {
        return hashFunc.newHasher().putInt(integer).hash().asLong();
    }
    
    
}
