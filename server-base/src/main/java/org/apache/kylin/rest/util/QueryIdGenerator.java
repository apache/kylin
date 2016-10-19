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

import org.apache.commons.lang3.time.FastDateFormat;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import java.util.concurrent.ThreadLocalRandom;

@ThreadSafe
public class QueryIdGenerator {
    private static final char[] base26 = "abcdefghijklmnopqrstuvwxyz".toCharArray();
    private static final FastDateFormat dateFormat = FastDateFormat.getInstance("yyyyMMdd_HHmmss");

    /**
     * @param project name of the project
     * @return the next query id. We try to generate unique id as much as possible, but don't guarantee it.
     */
    @Nonnull
    public String nextId(final String project) {
        char[] postfix = new char[6];
        for (int i = 0; i < postfix.length; i++) {
            postfix[i] = base26[ThreadLocalRandom.current().nextInt(base26.length)];
        }

        return String.format("%s_%s_%s", dateFormat.format(System.currentTimeMillis()), project, new String(postfix));
    }
}
