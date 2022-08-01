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

package org.apache.kylin.query.udf.dateUdf;

import java.sql.Date;
import java.sql.Timestamp;

import org.apache.calcite.linq4j.function.Parameter;
import org.apache.calcite.sql.type.NotConstant;
import org.apache.kylin.common.exception.CalciteNotSupportException;

public class UnixTimestampUDF implements NotConstant {

    public Long UNIX_TIMESTAMP() throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public Long UNIX_TIMESTAMP(@Parameter(name = "str1") String str1) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public Long UNIX_TIMESTAMP(@Parameter(name = "str1") String str1, @Parameter(name = "str2") String str2)
            throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public Long UNIX_TIMESTAMP(@Parameter(name = "date") Date date) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public Long UNIX_TIMESTAMP(@Parameter(name = "date") Date date, @Parameter(name = "str2") String str2)
            throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public Long UNIX_TIMESTAMP(@Parameter(name = "ts") Timestamp ts) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public Long UNIX_TIMESTAMP(@Parameter(name = "ts") Timestamp ts, @Parameter(name = "str2") String str2)
            throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }
}
