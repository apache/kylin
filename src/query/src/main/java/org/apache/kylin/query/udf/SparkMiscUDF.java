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

public class SparkMiscUDF implements NotConstant {
    public Long CRC32(@Parameter(name = "str1") String exp) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public Object EXPLODE(@Parameter(name = "t1") Long[] exp1) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public Boolean ISNULL(@Parameter(name = "exp") Object exp) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public String MD5(@Parameter(name = "str1") String exp) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public Object NVL(@Parameter(name = "left") Object left, @Parameter(name = "right") Object right)
            throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public Object NVL2(@Parameter(name = "exp") Object exp, @Parameter(name = "left") Object left,
            @Parameter(name = "right") Object right) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public String SHA(@Parameter(name = "str1") String exp) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public String SHA1(@Parameter(name = "str1") String exp) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public String SHA2(@Parameter(name = "str1") String exp1, @Parameter(name = "str2") Integer exp2)
            throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }
}
