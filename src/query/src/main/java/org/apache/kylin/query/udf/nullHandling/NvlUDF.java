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

package org.apache.kylin.query.udf.nullHandling;

import java.sql.Date;
import java.sql.Timestamp;

import org.apache.calcite.linq4j.function.Parameter;

public class NvlUDF {

    public String NVL(@Parameter(name = "str1") String expression1, @Parameter(name = "str2") String expression2) {
        return expression1 == null ? expression2 : expression1;
    }

    public Integer NVL(@Parameter(name = "num1") Integer expression1, @Parameter(name = "num2") Integer expression2) {
        return expression1 == null ? expression2 : expression1;
    }

    public Double NVL(@Parameter(name = "num1") Double expression1, @Parameter(name = "num2") Double expression2) {
        return expression1 == null ? expression2 : expression1;
    }

    public Date NVL(@Parameter(name = "date1") Date expression1, @Parameter(name = "date2") Date expression2) {
        return expression1 == null ? expression2 : expression1;
    }

    public Timestamp NVL(@Parameter(name = "date1") Timestamp expression1,
            @Parameter(name = "date2") Timestamp expression2) {
        return expression1 == null ? expression2 : expression1;
    }

    public Boolean NVL(@Parameter(name = "num1") Boolean expression1, @Parameter(name = "num2") Boolean expression2) {
        return expression1 == null ? expression2 : expression1;
    }

    public Long NVL(@Parameter(name = "num1") Long expression1, @Parameter(name = "num2") Long expression2) {
        return expression1 == null ? expression2 : expression1;
    }
}
