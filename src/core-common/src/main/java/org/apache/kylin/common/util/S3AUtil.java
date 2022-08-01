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



import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;

public class S3AUtil {
    public static final String ASSUMED_ROLE_CREDENTIAL_PROVIDER_NAME = "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider";
    public static final String INSTANCE_CREDENTIAL_PROVIDER_NAME = "com.amazonaws.auth.InstanceProfileCredentialsProvider";
    public static final String CREDENTIAL_PROVIDER_KEY_FORMAT = "fs.s3a.bucket.%s.aws.credentials.provider";
    public static final String ROLE_ARN_KEY_FORMAT = "fs.s3a.bucket.%s.assumed.role.arn";
    public static final String ROLE_CREDENTIAL_PROVIDER_KEY_FORMAT = "fs.s3a.bucket.%s.assumed.role.credentials.provider";
    public static final String S3_ENDPOINT_KEY_FORMAT = "fs.s3a.bucket.%s.endpoint";

    private S3AUtil() {
    }

    public static Map<String, String> generateRoleCredentialConfByBucketAndRoleAndEndpoint(String bucket, String role,
            String endpoint) {
        Map<String, String> conf = new HashMap<>();
        if (StringUtils.isNotEmpty(role)) {
            conf.put(String.format(ROLE_ARN_KEY_FORMAT, bucket), role);
            conf.put(String.format(CREDENTIAL_PROVIDER_KEY_FORMAT, bucket), ASSUMED_ROLE_CREDENTIAL_PROVIDER_NAME);
            conf.put(String.format(ROLE_CREDENTIAL_PROVIDER_KEY_FORMAT, bucket), INSTANCE_CREDENTIAL_PROVIDER_NAME);
        }
        if (StringUtils.isNotEmpty(endpoint)) {
            conf.put(String.format(S3_ENDPOINT_KEY_FORMAT, bucket), endpoint);
        }
        return conf;
    }
}
