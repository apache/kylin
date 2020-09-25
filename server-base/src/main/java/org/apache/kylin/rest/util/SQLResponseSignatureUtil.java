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

package org.apache.kylin.rest.util;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.rest.response.SQLResponse;
import org.apache.kylin.rest.signature.FactTableRealizationSetCalculator;
import org.apache.kylin.rest.signature.SignatureCalculator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.base.Preconditions;

public class SQLResponseSignatureUtil {

    public static final Logger logger = LoggerFactory.getLogger(SQLResponseSignatureUtil.class);

    public static boolean checkSignature(KylinConfig config, SQLResponse sqlResponse, String projectName) {
        String old = sqlResponse.getSignature();
        if (old == null) {
            return false;
        }
        String latest = createSignature(config, sqlResponse, projectName);
        return old.equals(latest);
    }

    public static String createSignature(KylinConfig config, SQLResponse sqlResponse, String projectName) {
        ProjectInstance project = ProjectManager.getInstance(config).getProject(projectName);
        Preconditions.checkNotNull(project);

        SignatureCalculator signatureCalculator;
        try {
            Class signatureClass = getSignatureClass(project.getConfig());
            signatureCalculator = (SignatureCalculator) signatureClass.getConstructor().newInstance();
        } catch (Exception e) {
            logger.warn("Will use default signature since fail to construct signature due to " + e);
            signatureCalculator = new FactTableRealizationSetCalculator();
        }
        return signatureCalculator.calculateSignature(config, sqlResponse, project);
    }

    private static Class getSignatureClass(KylinConfig config) {
        try {
            return Class.forName(config.getSQLResponseSignatureClass());
        } catch (ClassNotFoundException e) {
            logger.warn("Will use default signature since cannot find class " + config.getSQLResponseSignatureClass());
            return FactTableRealizationSetCalculator.class;
        }
    }
}
