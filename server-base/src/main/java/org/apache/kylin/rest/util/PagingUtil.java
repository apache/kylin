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

import java.util.Collections;
import java.util.List;

public class PagingUtil {

    public static <T> List<T> cutPage(List<T> full, int offset, int limit) {
        if (full == null)
            return null;

        int begin = offset;
        int end = offset + limit;

        return cut(full, begin, end);
    }

    private static <T> List<T> cut(List<T> full, int begin, int end) {
        if (begin >= full.size())
            return Collections.emptyList();

        if (end > full.size())
            end = full.size();

        return full.subList(begin, end);
    }
}
