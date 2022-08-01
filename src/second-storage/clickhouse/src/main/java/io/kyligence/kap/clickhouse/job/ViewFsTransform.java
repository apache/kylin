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

package io.kyligence.kap.clickhouse.job;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.Singletons;
import org.apache.kylin.common.exception.CommonErrorCode;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.HadoopUtil;

import java.io.IOException;
import java.net.URI;

// transform a viewFs url to a hdfs url
public class ViewFsTransform {
    private FileSystem vfs;
    private ViewFsTransform(FileSystem vfs) {
        this.vfs = vfs;
    }

    public static ViewFsTransform getInstance() {
        return Singletons.getInstance(ViewFsTransform.class, vClass -> {
            Configuration conf = HadoopUtil.getCurrentConfiguration();
            URI root = FileSystem.getDefaultUri(conf);
            FileSystem vfs = null;
            try{
                vfs = FileSystem.get(root, conf);
            }catch (IOException e){
                return ExceptionUtils.rethrow(new KylinException(CommonErrorCode.UNKNOWN_ERROR_CODE, e));
            }
            return new ViewFsTransform(vfs);
        });
    }

    public String generateFileUrl(
            String sourceUrl) {
        try {
            Path p = vfs.resolvePath(new Path(sourceUrl));
            return p.toString();
        }catch (IllegalArgumentException | IOException e) {
            return ExceptionUtils.rethrow(new KylinException(CommonErrorCode.UNKNOWN_ERROR_CODE, e));
        }
    }
}
