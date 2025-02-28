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

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.util.FileSystemUtil;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.junit.jupiter.api.Test;

@MetadataInfo(onlyProps = true)
class ObsConfigTest {

    @Test
    void getByLocation() {
        ObsConfig oss = ObsConfig.OSS;
        assert !ObsConfig.getByType(null).isPresent();
        assert !ObsConfig.getByType("Foo").isPresent();
        assert ObsConfig.getByType("oss").get().equals(oss);
        assert ObsConfig.getByLocation("oss://bucket/path").get() == oss;
        assert ObsConfig.getByLocation("s3a://bucket/path").get() == ObsConfig.S3;
        assert !ObsConfig.getByLocation("/path").isPresent();
        Map<String, String> credentialConf = FileSystemUtil.generateRoleCredentialConf("oss", "bucket1", "role",
                "cn-hangzhou", "oss-cn-hangzhou.aliyuncs.com");
        assert credentialConf.get(String.format(ObsConfig.OSS.getRoleArnKey(), "bucket1")).equals("role");
        try {
            FileStatus[] s = FileSystemUtil.listStatus(FileSystem.getLocal(new Configuration()), new Path("/"));
            assert s.length > 0;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
