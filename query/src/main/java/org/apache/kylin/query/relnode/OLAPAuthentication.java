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
package org.apache.kylin.query.relnode;

import java.util.ArrayList;

/**
 * Created by wangcheng on 7/8/16.
 */
public class OLAPAuthentication {
    private String username;
    private ArrayList<String> roles = new ArrayList<>();

    public void parseUserInfo(String userInfo) {
        String[] info = userInfo.split(",");
        if (info.length > 0) //first element is username
            this.username = info[0];
        for (int i = 1; i < info.length; i++) //the remains should be roles which starts from index 1
            this.roles.add(info[i]);
    }

    public String getUsername() {
        return this.username;
    }

    public ArrayList<String> getRoles() {
        return this.roles;
    }
}
