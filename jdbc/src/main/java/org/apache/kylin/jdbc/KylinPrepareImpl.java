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

package org.apache.kylin.jdbc;

import java.util.ArrayList;
import java.util.List;

import net.hydromatic.avatica.AvaticaParameter;
import net.hydromatic.avatica.ColumnMetaData;

/**
 * @author xduo
 * 
 */
public class KylinPrepareImpl implements KylinPrepare {

    @Override
    public PrepareResult prepare(String sql) {
        List<AvaticaParameter> aps = new ArrayList<AvaticaParameter>();

        int startIndex = 0;
        while (sql.indexOf("?", startIndex) >= 0) {
            AvaticaParameter ap = new AvaticaParameter(false, 0, 0, 0, null, null, null);
            aps.add(ap);
            startIndex = sql.indexOf("?", startIndex) + 1;
        }

        return new KylinPrepare.PrepareResult(sql, aps, null, ColumnMetaData.struct(new ArrayList<ColumnMetaData>()));
    }

}
