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

public class TableauStringUDF {

    public Integer ASCII(@Parameter(name = "str1") String s) {
        if (s == null) {
            return null;
        }
        return s.codePointAt(0);
    }

    public String CHR(@Parameter(name = "int1") Integer i) {
        if (i == null) {
            return null;
        }
        char c = (char) i.intValue();
        return String.valueOf(c);
    }

    public String SPACE(@Parameter(name = "int1") Integer a) {
        StringBuilder sb = new StringBuilder();
        while (a-- > 0) {
            sb.append(' ');
        }
        return sb.toString();
    }

}
