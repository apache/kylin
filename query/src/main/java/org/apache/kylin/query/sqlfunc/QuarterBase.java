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

package org.apache.kylin.query.sqlfunc;

import net.hydromatic.optiq.runtime.SqlFunctions;
import net.hydromatic.optiq.runtime.SqlFunctions.TimeUnitRange;

/**
 * @author xjiang
 * 
 */
public abstract class QuarterBase {

    /**
     * According to jvm spec, it return self method before parent.
     * So, we keep Date in parent and int in child
     */
    public static long eval(int date) {
        long month = SqlFunctions.unixDateExtract(TimeUnitRange.MONTH, date);
        return month / 4 + 1;
    }
}
