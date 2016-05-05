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

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;

/**
 * Created by dongli on 5/5/16.
 */
public class ResourceStoreUtil {
    public static void copy(KylinConfig srcConfig, KylinConfig dstConfig, List<String> paths) throws Exception {
        ResourceStore src = ResourceStore.getStore(srcConfig);
        ResourceStore dst = ResourceStore.getStore(dstConfig);
        for (String path : paths) {
            rCopy(src, dst, path);
        }
    }

    public static void rCopy(ResourceStore src, ResourceStore dst, String path) throws Exception {
        Method listResourceMethod = ResourceStore.class.getMethod("listResources", String.class);
        Iterable<String> children = (Iterable<String>) listResourceMethod.invoke(src, path);

        // case of resource (not a folder)
        if (children == null) {
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
        }
        // case of folder
        else {
            for (String child : children)
                rCopy(src, dst, child);
        }
    }
}
