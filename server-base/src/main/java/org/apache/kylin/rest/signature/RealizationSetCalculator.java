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

import java.security.MessageDigest;
import java.util.Set;

import org.apache.commons.codec.binary.Base64;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.response.SQLResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.base.CharMatcher;
import org.apache.kylin.shaded.com.google.common.base.Splitter;
import org.apache.kylin.shaded.com.google.common.base.Strings;
import org.apache.kylin.shaded.com.google.common.collect.Iterables;
import org.apache.kylin.shaded.com.google.common.collect.Sets;

public class RealizationSetCalculator implements SignatureCalculator {

    public static final Logger logger = LoggerFactory.getLogger(RealizationSetCalculator.class);

    @Override
    public String calculateSignature(KylinConfig config, SQLResponse sqlResponse, ProjectInstance project) {
        Set<String> realizations = getRealizations(config, sqlResponse.getCube(), project);
        if (realizations == null) {
            return null;
        }
        Set<RealizationSignature> signatureSet = Sets.newTreeSet();
        for (String realization : realizations) {
            RealizationSignature realizationSignature = getRealizationSignature(config, realization);
            if (realizationSignature != null) {
                signatureSet.add(realizationSignature);
            }
        }
        if (signatureSet.isEmpty()) {
            return null;
        }
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] signature = md.digest(signatureSet.toString().getBytes("UTF-8"));
            return new String(Base64.encodeBase64(signature), "UTF-8");
        } catch (Exception e) {
            logger.warn("Failed to calculate signature due to " + e);
            return null;
        }
    }

    protected Set<String> getRealizations(KylinConfig config, String cubes, ProjectInstance project) {
        if (Strings.isNullOrEmpty(cubes)) {
            return null;
        }
        String[] realizations = parseNamesFromCanonicalNames(StringUtil.splitByComma(cubes));
        return Sets.newHashSet(realizations);
    }

    protected static RealizationSignature getRealizationSignature(KylinConfig config, String realizationName) {
        RealizationSignature result = RealizationSignature.HybridSignature.getHybridSignature(config, realizationName);
        if (result == null) {
            result = RealizationSignature.CubeSignature.getCubeSignature(config, realizationName);
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
}
