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

package org.apache.kylin.gridtable;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.dict.Dictionary;

public class DefaultGTComparator implements IGTComparator {
    @Override
    public boolean isNull(ByteArray code) {
        // all 0xff is null
        byte[] array = code.array();
        for (int i = 0, j = code.offset(), n = code.length(); i < n; i++, j++) {
            if (array[j] != Dictionary.NULL)
                return false;
        }
        return true;
    }

    @Override
    public int compare(ByteArray code1, ByteArray code2) {
        return code1.compareTo(code2);
    }
}
