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

package org.apache.kylin.query.udf.formatUdf;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.Locale;

import org.apache.calcite.linq4j.function.Parameter;

public class DateFormatUDF {

    public String DATE_FORMAT(@Parameter(name = "date") Timestamp date, @Parameter(name = "part") String part) {
        String partOfDate = null;
        switch (part.toUpperCase(Locale.ROOT)) {
        case "YEAR":
            partOfDate = date.toString().substring(0, 4);
            break;
        case "MONTH":
            partOfDate = date.toString().substring(5, 7);
            break;
        case "DAY":
            partOfDate = date.toString().substring(8, 10);
            break;
        case "HOUR":
            partOfDate = date.toString().substring(11, 13);
            break;
        case "MINUTE":
        case "MINUTES":
            partOfDate = date.toString().substring(14, 16);
            break;
        case "SECOND":
        case "SECONDS":
            partOfDate = date.toString().substring(17, 19);
            break;
        default:
            //throws
        }
        return partOfDate;
    }

    public String DATE_FORMAT(@Parameter(name = "date") Date date, @Parameter(name = "part") String part) {
        return DATE_FORMAT(new Timestamp(date.getTime()), part);
    }
}
