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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.StringUtils;

public class PagingUtil {

    public static <T> List<T> cutPage(List<T> full, int pageOffset, int pageSize) {
        if (full == null)
            return null;

        int begin = pageOffset * pageSize;
        int end = begin + pageSize;

        return cut(full, begin, end);
    }

    private static <T> List<T> cut(List<T> full, int begin, int end) {
        if (begin >= full.size())
            return Collections.emptyList();

        if (end > full.size())
            end = full.size();

        return full.subList(begin, end);
    }

    public static List<String> getIdentifierAfterFuzzyMatching(String nameSeg, boolean isCaseSensitive,
            Collection<String> l) {
        List<String> identifier = new ArrayList<>();
        if (StringUtils.isBlank(nameSeg)) {
            identifier.addAll(l);
        } else {
            for (String u : l) {
                if (!isCaseSensitive && StringUtils.containsIgnoreCase(u, nameSeg)) {
                    identifier.add(u);
                }
                if (isCaseSensitive && StringUtils.contains(u, nameSeg)) {
                    identifier.add(u);
                }
            }
        }
        Collections.sort(identifier);
        return identifier;
    }

    public static boolean isInCurrentPage(int totalSize, int offset, int limit) {
        return totalSize >= offset * limit && totalSize < (offset * limit + limit);
    }
}
