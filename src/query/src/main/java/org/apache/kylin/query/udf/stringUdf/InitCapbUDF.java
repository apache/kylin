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

package org.apache.kylin.query.udf.stringUdf;

import org.apache.calcite.linq4j.function.Parameter;

public class InitCapbUDF {

    public String INITCAPB(@Parameter(name = "str") String s) {
        final int len = s.length();
        boolean start = true;
        final StringBuilder newS = new StringBuilder();

        for (int i = 0; i < len; i++) {
            char curCh = s.charAt(i);
            final int c = (int) curCh;
            if (start) { // curCh is whitespace or first character of word.
                if (isNumber(c)) {
                    start = false;
                } else if (isUppercase(c)) {
                    start = false;
                } else if (isLowerCase(c)) {
                    start = false;
                    curCh = (char) (c - 32); // Uppercase this character
                }
            } else { // Inside of a word or white space after end of word.
                if (isNumber(c)) {
                    // noop
                } else if (isUppercase(c)) {
                    curCh = (char) (c + 32); // Lowercase this character
                } else if (isLowerCase(c)) {
                    // noop
                } else { // whitespace
                    start = true;
                }
            }
            newS.append(curCh);
        } // for each character in s
        return newS.toString();
    }

    public Boolean isNumber(int c) {
        return c > 47 && c < 58;
    }

    public Boolean isUppercase(int c) {
        return c > 64 && c < 91;
    }

    public Boolean isLowerCase(int c) {
        return c > 96 && c < 123;
    }

}
