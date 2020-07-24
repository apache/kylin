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
package org.apache.kylin.common.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CliCommandExecutorTest {

    private String[][] commands = {
            {"nslookup unknown.com &", "nslookupunknown.com"},
            {"cat `whoami`", "catwhoami"},
            {"echo \"kylin@headnode:/home/kylin/lib/job.jar?key=Value123\",", "echo\"kylin@headnode:/home/kylin/lib/job.jar?key=Value123\","},
            {"whoami > /var/www/static/whoami.txt", "whoami/var/www/static/whoami.txt"},
            {"mysql_test@jdbc,url=jdbc:mysql://localhost:3306/kylin,username=kylin_test,password=bUmSqT/opyqz89Geu0yQ3g==,maxActive=10,maxIdle=10,passwordEncrypted=true", "mysql_test@jdbc,url=jdbc:mysql://localhost:3306/kylin,username=kylin_test,password=bUmSqT/opyqz89Geu0yQ3g==,maxActive=10,maxIdle=10,passwordEncrypted=true"},
            {"c1 || c2# || c3 || *c4\\", "c1c2c3c4"},
            {"c1 &&", "c1"},
            {"c1 + > c2 [p1]%", "c1c2[p1]%"},
            {"c1 | ${c2}", "c1c2"},
    };

    private String[][] properties = {
            {"default;show tables", "defaultshowtables"},
            {"default_kylin;drop tables;", "default_kylindroptables"},
            {"db and 1=2", "dband12"}
    };

    @Test
    public void testCmd() {
        for (String[] pair : commands) {
            assertEquals(pair[1], CliCommandExecutor.checkParameter(pair[0]));
        }
    }

    @Test
    public void testCmd2() {
        for (String[] pair : commands) {
            assertEquals(pair[1], CliCommandExecutor.checkParameterWhiteList(pair[0]));
        }
    }

    @Test
    public void testHiveProperties() {
        for (String[] pair : properties) {
            assertEquals(pair[1], CliCommandExecutor.checkHiveProperty(pair[0]));
        }
    }
}
