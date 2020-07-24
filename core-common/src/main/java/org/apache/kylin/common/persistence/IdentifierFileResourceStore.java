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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.base.Preconditions;

/**
 * it need identifier to transfer relative path to absolute path when building cube like reading global dict and
 * using path to locate the data and make use of it, so this ResourceStore separate identifier from the data path
 * saved in params.
 *
 */
@SuppressWarnings("unused")
public class IdentifierFileResourceStore extends FileResourceStore {
    private static final Logger logger = LoggerFactory.getLogger(IdentifierFileResourceStore.class);

    private static final String IFILE_SCHEME = "ifile";

    public IdentifierFileResourceStore(KylinConfig kylinConfig) throws Exception {
        super(kylinConfig);
    }

    protected String getPath(KylinConfig kylinConfig) {
        StorageURL metadataUrl = kylinConfig.getMetadataUrl();
        Preconditions.checkState(IFILE_SCHEME.equals(metadataUrl.getScheme()));
        return metadataUrl.getParameter("path");
    }
}
