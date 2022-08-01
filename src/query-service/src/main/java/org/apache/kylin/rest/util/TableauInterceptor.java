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

package org.apache.kylin.rest.util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;
import org.apache.kylin.rest.response.SQLResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableauInterceptor {

    protected static final Logger logger = LoggerFactory.getLogger(TableauInterceptor.class);

    private static String[] tableauTestQueries = new String[] { "SELECT 1", //
            "CREATE LOCAL TEMPORARY TABLE \"XTableau_B_Connect\" ( \"COL\" INTEGER ) ON COMMIT PRESERVE ROWS", //
            "DROP TABLE \"XTableau_B_Connect\"", //
            "SELECT \"COL\" FROM (SELECT 1 AS \"COL\") AS \"SUBQUERY\"", //
            "SELECT TOP 1 \"COL\" FROM (SELECT 1 AS \"COL\") AS \"CHECKTOP\"", //
            "SELECT \"COL\" FROM (SELECT 1 AS \"COL\") AS \"CHECKTOP\" LIMIT 1", //
            "SELECT \"SUBCOL\" AS \"COL\"  FROM (   SELECT 1 AS \"SUBCOL\" ) \"SUBQUERY\" GROUP BY 1", //
            "SELECT \"SUBCOL\" AS \"COL\" FROM (   SELECT 1 AS \"SUBCOL\" ) \"SUBQUERY\" GROUP BY 2", //
            "INSERT INTO \"XTableau_C_Connect\" SELECT * FROM (SELECT 1 AS COL) AS CHECKTEMP LIMIT 1", //
            "DROP TABLE \"XTableau_C_Connect\"", //
            "INSERT INTO \"XTableau_B_Connect\" SELECT * FROM (SELECT 1 AS COL) AS CHECKTEMP LIMIT 1" };

    private static SQLResponse temp = new SQLResponse(new LinkedList<SelectedColumnMeta>() {
        private static final long serialVersionUID = -8086728462624901359L;

        {
            add(new SelectedColumnMeta(false, false, true, false, 2, true, 11, "COL", "COL", "", "", "", 10, 0, 4,
                    "int4", false, true, false));
        }
    }, new LinkedList<List<String>>() {
        private static final long serialVersionUID = -470083340592928073L;

        {
            add(new LinkedList<String>() {
                private static final long serialVersionUID = -3673192785838230054L;

                {
                    add("1");
                }
            });
        }
    }, 0, false, null);

    private static SQLResponse[] fakeResponses = new SQLResponse[] { temp, //
            new SQLResponse(null, null, 0, false, null), //
            new SQLResponse(null, null, 0, false, null), //
            temp, //
            new SQLResponse(null, null, 0, true, "near 1 syntax error"), //
            temp, //
            new SQLResponse(null, null, 0, true, "group by 1????"), //
            new SQLResponse(null, null, 0, true, "group by 2????"), //
            new SQLResponse(null, null, 0, true, "XTableau_C_Connect not exist"), //
            new SQLResponse(null, null, 0, true, "XTableau_C_Connect not exist"), //
            new SQLResponse(null, null, 0, true, "XTableau_B_Connect not exist"), };

    private static ArrayList<HashSet<String>> tableauTestQueriesInToken = new ArrayList<HashSet<String>>();

    static {
        for (String q : tableauTestQueries) {
            HashSet<String> temp = new HashSet<String>();
            for (String token : q.split("[\r\n\t \\(\\)]")) {
                temp.add(token);
            }
            temp.add("");
            tableauTestQueriesInToken.add(temp);
        }
    }

    public static SQLResponse tableauIntercept(String sql) {

        String[] tokens = sql.split("[\r\n\t \\(\\)]");
        for (int i = 0; i < tableauTestQueries.length; ++i) {
            if (isTokenWiseEqual(tokens, tableauTestQueriesInToken.get(i))) {
                logger.info("Hit fake response {}", i);
                return fakeResponses[i];
            }
        }

        return null;
    }

    private static boolean isTokenWiseEqual(String[] tokens, HashSet<String> tokenSet) {
        for (String token : tokens) {
            if (!tokenSet.contains(token)) {
                return false;
            }
        }
        return true;
    }

}
