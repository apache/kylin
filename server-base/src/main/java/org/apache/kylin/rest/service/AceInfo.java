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

package org.apache.kylin.rest.service;

import java.io.Serializable;

import org.springframework.security.acls.model.AccessControlEntry;

/**
 * Created by xiefan on 17-5-2.
 */
class AceInfo implements Serializable {
    private SidInfo sidInfo;
    private int permissionMask;

    public AceInfo() {
    }

    public AceInfo(AccessControlEntry ace) {
        super();
        this.sidInfo = new SidInfo(ace.getSid());
        this.permissionMask = ace.getPermission().getMask();
    }

    public SidInfo getSidInfo() {
        return sidInfo;
    }

    public void setSidInfo(SidInfo sidInfo) {
        this.sidInfo = sidInfo;
    }

    public int getPermissionMask() {
        return permissionMask;
    }

    public void setPermissionMask(int permissionMask) {
        this.permissionMask = permissionMask;
    }

}
