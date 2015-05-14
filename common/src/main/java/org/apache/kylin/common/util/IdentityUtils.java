package org.apache.kylin.common.util;

import java.util.Collection;
import java.util.IdentityHashMap;

/**
 * Created by Hongbin Ma(Binmahone) on 5/14/15.
 */
public class IdentityUtils {
    public static <K> boolean collectionReferenceEquals(Collection<K> collectionA, Collection<K> collectionB) {
        if (collectionA == null || collectionB == null) {
            throw new RuntimeException("input must be not null");
        }

        IdentityHashMap<K, Void> mapA = new IdentityHashMap<>();
        IdentityHashMap<K, Void> mapB = new IdentityHashMap<>();
        for (K key : collectionA) {
            mapA.put(key, null);
        }
        for (K key : collectionB) {
            mapB.put(key, null);
        }

        if (mapA.keySet().size() != mapB.keySet().size()) {
            return false;
        }

        for (K key : mapA.keySet()) {
            if (!mapB.keySet().contains(key)) {
                return false;
            }
        }
        return true;
    }
}
