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
 *
 */

package org.apache.kylin.tool.util;

import java.lang.reflect.Method;
import java.util.Collection;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.metadata.MetadataConstants;

/**
 * Created by dongli on 5/5/16.
 */
public class ResourceStoreUtil {
    public static void copy(KylinConfig srcConfig, KylinConfig dstConfig, Collection<String> paths) throws Exception {
        ResourceStore src = ResourceStore.getStore(srcConfig);
        ResourceStore dst = ResourceStore.getStore(dstConfig);
        for (String path : paths) {
            rCopy(src, dst, path);
        }
    }

    public static void rCopy(ResourceStore src, ResourceStore dst, String path) throws Exception {
        Method listResourceMethod = ResourceStore.class.getMethod("listResources", String.class);
        Iterable<String> children = (Iterable<String>) listResourceMethod.invoke(src, path);

        if (children == null) {
            // case of resource (not a folder)
            try {
                RawResource res = src.getResource(path);
                if (res != null) {
                    dst.putResource(path, res.inputStream, res.timestamp);
                    res.inputStream.close();
                } else {
                    System.out.println("Resource not exist for " + path);
                }
            } catch (Exception ex) {
                System.err.println("Failed to open " + path);
                ex.printStackTrace();
            }
        } else {
            // case of folder
            for (String child : children)
                rCopy(src, dst, child);
        }
    }

    public static String concatCubeDescResourcePath(String descName) {
        return ResourceStore.CUBE_DESC_RESOURCE_ROOT + "/" + descName + MetadataConstants.FILE_SURFIX;
    }

    public static String concatCubeSegmentStatisticsResourcePath(String cubeName, String cubeSegmentId) {
        return ResourceStore.CUBE_STATISTICS_ROOT + "/" + cubeName + "/" + cubeSegmentId + ".seq";
    }

    public static String concatJobPath(String uuid) {
        return ResourceStore.EXECUTE_RESOURCE_ROOT + "/" + uuid;
    }

    public static String concatJobOutputPath(String uuid) {
        return ResourceStore.EXECUTE_OUTPUT_RESOURCE_ROOT + "/" + uuid;
    }
}
