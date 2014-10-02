/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kylinolap.rest.constant;

/**
 * @author xduo
 */
public class Constant {

    //@hardcode
    public final static String FakeSchemaName = "defaultSchema";

    //@hardcode
    public final static String FakeCatalogName = "defaultCatalog";

    public final static String IDENTITY_USER = "user";

    public final static String IDENTITY_ROLE = "role";

    public final static String ACCESS_HAS_ROLE_ADMIN = "hasRole('ROLE_ADMIN')";

    public final static String ACCESS_POST_FILTER_READ =
            "hasRole('ROLE_ADMIN') or hasPermission(filterObject, 'READ') or hasPermission(filterObject, 'MANAGEMENT') "
                    + "or hasPermission(filterObject, 'OPERATION') or hasPermission(filterObject, 'ADMINISTRATION')";

    public final static String SERVER_MODE_QUERY = "query";
    public final static String SERVER_MODE_JOB = "job";
    public final static String SERVER_MODE_ALL = "all";

}
