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

package org.apache.kylin.rest.constant;

import org.apache.kylin.common.annotation.ThirdPartyDependencies;

/**
 * @author xduo
 *
 */
public class Constant {

    public final static String FakeSchemaName = "defaultSchema";
    public final static String FakeCatalogName = "defaultCatalog";

    public final static String IDENTITY_USER = "user";
    public final static String ADMIN = "ADMIN";

    @ThirdPartyDependencies({
            @ThirdPartyDependencies.ThirdPartyDependent(repository = "static-user-manager", classes = {
                    "StaticUserService" }) })
    public final static String ROLE_ADMIN = "ROLE_ADMIN";
    public final static String ROLE_MODELER = "ROLE_MODELER";
    public final static String ROLE_ANALYST = "ROLE_ANALYST";

    public final static String GROUP_ALL_USERS = "ALL_USERS";

    public final static String ACCESS_HAS_ROLE_ADMIN = "hasRole('ROLE_ADMIN')";

    public static final String ACCESS_CAN_PROJECT_ADMIN = "hasRole('ROLE_ADMIN') " //
            + " or hasPermission(#project, 'ADMINISTRATION')";

    public static final String ACCESS_CAN_PROJECT_WRITE = ACCESS_CAN_PROJECT_ADMIN //
            + " or hasPermission(#project, 'MANAGEMENT')";

    public static final String ACCESS_CAN_PROJECT_OPERATION = ACCESS_CAN_PROJECT_WRITE //
            + " or hasPermission(#project, 'OPERATION')";

    public static final String ACCESS_POST_FILTER_READ = ACCESS_CAN_PROJECT_OPERATION //
            + " or hasPermission(#project, 'READ')";

    public static final String ACCESS_POST_FILTER_READ_FOR_DATA_PERMISSION_SEPARATE = "hasPermission(#project, 'DATA_QUERY')";

    public static final String ACCESS_CAN_PROJECT_OPERATION_DESIGN = ACCESS_CAN_PROJECT_WRITE //
            + " or (hasPermission(#project, 'OPERATION') and #isIndexEnableOperatorDesign)";
}
