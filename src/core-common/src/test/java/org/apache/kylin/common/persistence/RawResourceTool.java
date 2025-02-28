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
package org.apache.kylin.common.persistence;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import org.apache.kylin.common.persistence.resources.ProjectRawResource;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.guava30.shaded.common.io.ByteSource;

/**
 * 23/01/2024 hellozepp(lisheng.zhanglin@163.com)
 */
public class RawResourceTool {
    public static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
    public static ProjectRawResource createProjectRawResource(String projectName, long mvcc, String uuid) {
        ProjectRawResource resource = new ProjectRawResource();
        resource.setMetaKey(projectName);
        resource.setName(projectName);
        resource.setUuid(uuid);
        resource.setContent(createContent(projectName, uuid));
        resource.setTs(System.currentTimeMillis());
        resource.setMvcc(mvcc);
        return resource;
    }

    public static ProjectRawResource createProjectRawResource(String projectName, long mvcc) {
        return createProjectRawResource(projectName, mvcc, UUID.randomUUID().toString());
    }

    public static byte[] createContent(String projectName, String uuid) {
        return ("{ \"uuid\" : \"" + uuid + "\",\"meta_key\" : \"" + projectName + "\",\"name\" : \"" + projectName
                + "\"}").getBytes(DEFAULT_CHARSET);
    }

    public static ByteSource createByteSourceByPath(String path) {
        Pair<MetadataType, String> pair = MetadataType.splitKeyWithType(path);
        String projectName = pair.getSecond();
        return ByteSource.wrap(("{ \"uuid\" : \"" + UUID.randomUUID() + "\",\"meta_key\" : \"" + projectName
                + "\",\"name\" : \"" + projectName + "\"}").getBytes(DEFAULT_CHARSET));
    }

    public static ByteSource createByteSource(String projectName) {
        return ByteSource.wrap(("{ \"uuid\" : \"" + UUID.randomUUID() + "\",\"meta_key\" : \"" + projectName
                + "\",\"name\" : \"" + projectName + "\"}").getBytes(DEFAULT_CHARSET));
    }

}
