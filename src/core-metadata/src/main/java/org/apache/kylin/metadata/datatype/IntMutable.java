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

package org.apache.kylin.metadata.datatype;

import java.io.Serializable;

@SuppressWarnings("serial")
public class IntMutable implements Comparable<IntMutable>, Serializable {

    private int v;

    public IntMutable() {
        this(0);
    }

    public IntMutable(int v) {
        set(v);
    }

    public int get() {
        return v;
    }

    public void set(int v) {
        this.v = v;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof IntMutable)) {
            return false;
        }
        IntMutable other = (IntMutable) o;
        return this.v == other.v;
    }

    @Override
    public int hashCode() {
        return (int) v;
    }

    @Override
    public int compareTo(IntMutable o) {
        long thisValue = this.v;
        long thatValue = o.v;
        return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
    }

    @Override
    public String toString() {
        return Integer.toString(v);
    }

}
