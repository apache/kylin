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

import java.util.Arrays;

/**
 * @author yangli9
 */
public class ByteArray implements Comparable<ByteArray> {

    public byte[] data;

    public ByteArray(byte[] data) {
        this.data = data;
    }

    @Override
    public int hashCode() {
        return Bytes.hashCode(data);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        ByteArray other = (ByteArray) obj;
        if (!Arrays.equals(data, other.data))
            return false;
        return true;
    }

    @Override
    public int compareTo(ByteArray o) {
        return Bytes.compareTo(this.data, o.data);
    }
}
