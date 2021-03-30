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

import java.util.Locale;

/**
 * A string wrapper that makes .equals a caseInsensitive match
 * <p>
 *     a collection that wraps a String mapping in CaseInsensitiveStrings will still accept a String but will now
 *     return a caseInsensitive match rather than a caseSensitive one
 * </p>
 */
public class CaseInsensitiveString {
    String str;

    private CaseInsensitiveString(String str) {
        this.str = str;
    }

    public static CaseInsensitiveString wrap(String str) {
        return new CaseInsensitiveString(str);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null)
            return false;

        if (o.getClass() == getClass()) { //is another CaseInsensitiveString
            CaseInsensitiveString that = (CaseInsensitiveString) o;
            return (str != null) ? str.equalsIgnoreCase(that.str) : that.str == null;
        } else {
            return false;
        }

    }

    @Override
    public int hashCode() {
        return (str != null) ? str.toUpperCase(Locale.ROOT).hashCode() : 0;
    }

    @Override
    public String toString() {
        return str;
    }
}
