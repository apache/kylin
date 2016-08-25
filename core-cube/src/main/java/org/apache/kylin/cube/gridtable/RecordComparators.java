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

package org.apache.kylin.cube.gridtable;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.gridtable.IGTComparator;

public class RecordComparators {

    public static RecordComparator getRangeStartComparator(final IGTComparator comp) {
        return new RecordComparator(new ComparatorEx<ByteArray>() {
            @Override
            public int compare(ByteArray a, ByteArray b) {
                if (a.array() == null) {
                    if (b.array() == null) {
                        return 0;
                    } else {
                        return -1;
                    }
                } else if (b.array() == null) {
                    return 1;
                } else {
                    return comp.compare(a, b);
                }
            }
        });
    }

    public static RecordComparator getRangeEndComparator(final IGTComparator comp) {
        return new RecordComparator(new ComparatorEx<ByteArray>() {
            @Override
            public int compare(ByteArray a, ByteArray b) {
                if (a.array() == null) {
                    if (b.array() == null) {
                        return 0;
                    } else {
                        return 1;
                    }
                } else if (b.array() == null) {
                    return -1;
                } else {
                    return comp.compare(a, b);
                }
            }
        });
    }

    public static RecordComparator getRangeStartEndComparator(final IGTComparator comp) {
        return new AsymmetricRecordComparator(new ComparatorEx<ByteArray>() {
            @Override
            public int compare(ByteArray a, ByteArray b) {
                if (a.array() == null || b.array() == null) {
                    return -1;
                } else {
                    return comp.compare(a, b);
                }
            }
        });
    }

}
