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

package org.apache.kylin.rest.security;

/**
 * Created by xiefan on 17-5-3.
 */
public interface AclConstant {

    static final String ACL_INFO_FAMILY = "i";

    static final String ACL_ACES_FAMILY = "a";

    static final String ACL_TABLE_NAME = "_acl";

    static final String USER_AUTHORITY_FAMILY = "a";

    static final String USER_TABLE_NAME = "_user";

    static final String USER_AUTHORITY_COLUMN = "c";

    static String ACL_INFO_FAMILY_TYPE_COLUMN = "t";

    static String ACL_INFO_FAMILY_OWNER_COLUMN = "o";

    static String ACL_INFO_FAMILY_PARENT_COLUMN = "p";

    static String ACL_INFO_FAMILY_ENTRY_INHERIT_COLUMN = "i";

    static final String PWD_PREFIX = "PWD:";
}
