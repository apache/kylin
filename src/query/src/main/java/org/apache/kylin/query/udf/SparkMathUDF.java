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

package org.apache.kylin.query.udf;

import org.apache.calcite.linq4j.function.Parameter;
import org.apache.calcite.sql.type.NotConstant;
import org.apache.kylin.common.exception.CalciteNotSupportException;

public class SparkMathUDF implements NotConstant {

    public Object BROUND(@Parameter(name = "str1") Double exp1, @Parameter(name = "str2") Integer exp2)
            throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public Double CBRT(@Parameter(name = "str1") Double exp) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public String CONV(@Parameter(name = "str1") String exp1, @Parameter(name = "str2") Integer exp2,
            @Parameter(name = "str3") Integer exp3) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public String CONV(@Parameter(name = "str1") Long exp1, @Parameter(name = "str2") Integer exp2,
            @Parameter(name = "str3") Integer exp3) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public String CONV(@Parameter(name = "str1") double exp1, @Parameter(name = "str2") Integer exp2,
            @Parameter(name = "str3") Integer exp3) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public Double COSH(@Parameter(name = "str1") Double exp) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public Double EXPM1(@Parameter(name = "str1") Double exp) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public Long FACTORIAL(@Parameter(name = "str1") Integer exp) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public Long FACTORIAL(@Parameter(name = "str1") double exp) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public Double HYPOT(@Parameter(name = "str1") Double exp1, @Parameter(name = "str2") Double exp2)
            throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public Double LOG(@Parameter(name = "str1") Double exp1, @Parameter(name = "str2") Double exp2)
            throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public Double LOG1P(@Parameter(name = "str1") Double exp) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public Double LOG2(@Parameter(name = "str1") Double exp) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public Double RINT(@Parameter(name = "str1") Double exp) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public Double SINH(@Parameter(name = "str1") Double exp) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public Double TANH(@Parameter(name = "str1") Double exp) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }
}
