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

import java.util.List;

import org.apache.calcite.linq4j.function.Parameter;
import org.apache.kylin.common.exception.CalciteNotSupportException;

public class KylinBitmapUDF {
    public Long INTERSECT_COUNT_BY_COL(List maps) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public Object SUBTRACT_BITMAP_VALUE(@Parameter(name = "m1") byte[] map1, @Parameter(name = "m2") byte[] map2)
            throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public Object SUBTRACT_BITMAP_UUID(@Parameter(name = "m1") byte[] map1, @Parameter(name = "m2") byte[] map2)
            throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public Object BITMAP_UUID_TO_ARRAY(@Parameter(name = "m1") Object map1) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public Object SUBTRACT_BITMAP_UUID_COUNT(@Parameter(name = "left") Object left,
            @Parameter(name = "right") Object right) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public Object SUBTRACT_BITMAP_UUID_DISTINCT(@Parameter(name = "left") Object left,
            @Parameter(name = "right") Object right) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public Object SUBTRACT_BITMAP_UUID_VALUE_ALL(@Parameter(name = "left") Object left,
            @Parameter(name = "right") Object right) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public Object SUBTRACT_BITMAP_UUID_VALUE(@Parameter(name = "first") Object first,
            @Parameter(name = "second") Object second, @Parameter(name = "third") Object third,
            @Parameter(name = "four") Object four) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }
}
