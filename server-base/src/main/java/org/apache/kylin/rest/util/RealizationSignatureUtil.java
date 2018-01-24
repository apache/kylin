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

package org.apache.kylin.rest.util;

import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.rest.response.SQLResponse;
import org.apache.kylin.rest.util.RealizationSignature.CubeSignature;
import org.apache.kylin.rest.util.RealizationSignature.HybridSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CharMatcher;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class RealizationSignatureUtil {

    public static final Logger logger = LoggerFactory.getLogger(RealizationSignatureUtil.class);

    public enum ResultCode {
        CUBE_DELETED, CUBE_ADDED, CUBE_ENABLED, CUBE_DISABLED, SEGMENT_DELETED, SEGMENT_ADDED, SEGMENT_REFRESHED
    }

    public static Set<RealizationSignature> getSignature(KylinConfig config, String cubes) {
        if (Strings.isNullOrEmpty(cubes)) {
            return null;
        }
        String[] realizations = parseNamesFromCanonicalNames(cubes.split(","));
        Set<RealizationSignature> signatureSet = Sets.newHashSetWithExpectedSize(realizations.length);
        for (String realization : realizations) {
            RealizationSignature realizationSignature = getRealizationSignature(config, realization);
            if (realizationSignature != null) {
                signatureSet.add(realizationSignature);
            }
        }
        return signatureSet;
    }

    public static boolean checkSignature(KylinConfig config, SQLResponse sqlResponse) {
        Set<RealizationSignature> oldSignatures = sqlResponse.getSignature();
        Set<RealizationSignature> curSignatures = getSignature(config, sqlResponse.getCube());
        if (oldSignatures == null || curSignatures == null) {
            logger.warn("The cube info in sqlResponse is null.");
            return false;
        }
        if (!oldSignatures.equals(curSignatures)) {
            String explainMsg = config.isQuerySignatureDetailedExpiredReasonEnabled() //
                    ? getSignatureDetailedExpiredReason(oldSignatures, curSignatures) //
                    : "old signature is: " + oldSignatures + ",\n while current signature is: " + curSignatures;
            logger.debug(explainMsg);
            return false;
        }
        return true;
    }

    @VisibleForTesting
    static RealizationSignature getRealizationSignature(KylinConfig config, String realizationName) {
        RealizationSignature result = HybridSignature.getHybridSignature(config, realizationName);
        if (result == null) {
            result = CubeSignature.getCubeSignature(config, realizationName);
        }
        return result;
    }

    private static String[] parseNamesFromCanonicalNames(String[] canonicalNames) {
        String[] result = new String[canonicalNames.length];
        for (int i = 0; i < canonicalNames.length; i++) {
            result[i] = parseCanonicalName(canonicalNames[i]).getSecond();
        }
        return result;
    }

    /**
     * @param canonicalName
     * @return type and name pair for realization
     */
    private static Pair<String, String> parseCanonicalName(String canonicalName) {
        Iterable<String> parts = Splitter.on(CharMatcher.anyOf("[]=,")).split(canonicalName);
        String[] partsStr = Iterables.toArray(parts, String.class);
        return new Pair<>(partsStr[0], partsStr[2]);
    }

    private static String getSignatureDetailedExpiredReason(Set<RealizationSignature> oldSignatures,
            Set<RealizationSignature> curSignatures) {
        Map<String, RealizationSignature> oldSignatureMap = createSignatureMap(oldSignatures);
        for (RealizationSignature cur : curSignatures) {
            RealizationSignature old = oldSignatureMap.get(cur.getKey());
            if (old == null) {
                return "Realization " + cur.getKey() + " is " + ResultCode.CUBE_ADDED;
            }
            if (old.equals(cur)) {
                oldSignatureMap.remove(cur.getKey());
                continue;
            }
            return old instanceof HybridSignature ? //
                    getSignatureDetailedExpiredReason(//
                            ((HybridSignature) old).realizationSignatureSet, //
                            ((HybridSignature) cur).realizationSignatureSet)
                    : getCubeSignatureDetailedExpiredReason(//
                            (CubeSignature) old, //
                            (CubeSignature) cur);
        }
        for (RealizationSignature old : oldSignatureMap.values()) {
            return "Realization " + old.getKey() + " is " + ResultCode.CUBE_DELETED;
        }
        return null;
    }

    private static String getCubeSignatureDetailedExpiredReason(CubeSignature old, CubeSignature cur) {
        if (old.status != cur.status) {
            ResultCode resultCode = old.status == RealizationStatusEnum.READY ? ResultCode.CUBE_DISABLED
                    : ResultCode.CUBE_ENABLED;
            return "Realization " + old.getKey() + " is " + resultCode;
        }
        Map<String, SegmentSignature> oldMap = createSignatureMap(old.segmentSignatureSet);
        for (SegmentSignature curSeg : cur.segmentSignatureSet) {
            SegmentSignature oldSeg = oldMap.get(curSeg.getKey());
            if (oldSeg == null) {
                return "Segment " + cur.getKey() + "-" + curSeg.getKey() + " is " + ResultCode.SEGMENT_ADDED;
            }
            if (oldSeg.equals(curSeg)) {
                oldMap.remove(curSeg.getKey());
                continue;
            }
            return "Segment " + cur.getKey() + "-" + curSeg.getKey() + " is " + ResultCode.SEGMENT_REFRESHED;
        }
        for (SegmentSignature oldSeg : oldMap.values()) {
            return "Segment " + cur.getKey() + "-" + oldSeg.getKey() + " is " + ResultCode.SEGMENT_DELETED;
        }
        return null;
    }

    private static <T extends ComponentSignature> Map<String, T> createSignatureMap(Set<T> signatures) {
        Preconditions.checkNotNull(signatures);
        Map<String, T> signatureMap = Maps.newHashMapWithExpectedSize(signatures.size());
        for (T signature : signatures) {
            signatureMap.put(signature.getKey(), signature);
        }
        return signatureMap;
    }
}
