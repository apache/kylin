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

import java.security.SecureRandom;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.kylin.guava30.shaded.common.base.Preconditions;

import lombok.experimental.UtilityClass;

@UtilityClass
public class RandomUtil {
    private static final SecureRandom random = new SecureRandom();

    public static UUID randomUUID() {
        return new UUID(ThreadLocalRandom.current().nextLong(), ThreadLocalRandom.current().nextLong());
    }

    public static String randomUUIDStr() {
        return randomUUID().toString();
    }

    public static int nextInt(final int startInclusive, final int endExclusive) {
        Preconditions.checkArgument(endExclusive >= startInclusive,
                "Start value must be smaller or equal to end value.");
        Preconditions.checkArgument(startInclusive >= 0, "Both range values must be non-negative.");

        if (startInclusive == endExclusive) {
            return startInclusive;
        }

        return startInclusive + random.nextInt(endExclusive - startInclusive);
    }

    public static int nextInt(final int endExclusive) {
        return nextInt(0, endExclusive);
    }
}
