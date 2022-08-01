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

package org.apache.kylin.common.constant;

public class HttpConstant {

    private HttpConstant() {
    }

    /** kylin api client accept */
    public static final String HTTP_VND_APACHE_KYLIN_EARLY_JSON = "application/json";

    /** KAP 2x and 3x api client accept */
    public static final String HTTP_VND_APACHE_KYLIN_V2_JSON = "application/vnd.apache.kylin-v2+json";

    /** Some dirty compatibility client accept, delete it after KI use v4 api */
    public static final String HTTP_VND_APACHE_KYLIN_V3_JSON = "application/vnd.apache.kylin-v3+json";

    /** KAP 4x api client accept */
    public static final String HTTP_VND_APACHE_KYLIN_V4_JSON = "application/vnd.apache.kylin-v4+json";

    /** KAP 4x api client accept */
    public static final String HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON = "application/vnd.apache.kylin-v4-public+json";

    public static final String HTTP_VND_APACHE_KYLIN_JSON = HTTP_VND_APACHE_KYLIN_V4_JSON;
}
