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

package org.apache.kylin.query.security;

import java.util.Set;

import org.apache.commons.lang.StringUtils;

public class AccessDeniedException extends RuntimeException {
    public AccessDeniedException(String s) {
        super("Query failed, access " + s + " denied");
    }

    public AccessDeniedException(Set<String> unautorizedTables, Set<String> unautorizedColumns) {
        super(pasteToErrorMsg(unautorizedTables, unautorizedColumns));
    }

    private static String pasteToErrorMsg(Set<String> unautorizedTables, Set<String> unautorizedColumns) {
        StringBuilder errMsg = new StringBuilder("Query failed. You are not authorized to ");
        if (!unautorizedTables.isEmpty()) {
            String unautorizedTableStr = StringUtils.join(unautorizedTables, ",");
            errMsg.append("tables ");
            errMsg.append(unautorizedTableStr);
        }
        if (!unautorizedColumns.isEmpty()) {
            if (!unautorizedTables.isEmpty()) {
                errMsg.append(", ");
            }
            String unautorizedColumnStr = StringUtils.join(unautorizedColumns, ",");
            errMsg.append("columns ");
            errMsg.append(unautorizedColumnStr);
        }
        errMsg.append(".");
        return errMsg.toString();
    }
}
