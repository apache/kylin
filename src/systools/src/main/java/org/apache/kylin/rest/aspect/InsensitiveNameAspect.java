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
package org.apache.kylin.rest.aspect;

import java.util.Objects;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.security.AclEntityType;
import org.apache.kylin.metadata.insensitive.InsensitiveRequest;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.user.ManagedUser;
import io.kyligence.kap.metadata.user.NKylinUserManager;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.stereotype.Component;

import com.google.common.collect.Sets;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Aspect
@Component
public class InsensitiveNameAspect {

    public static String getCaseInsentiveType(String type) {
        Set<String> originTypes = Sets.newHashSet(MetadataConstants.TYPE_USER, MetadataConstants.TYPE_GROUP,
                AclEntityType.N_DATA_MODEL, AclEntityType.PROJECT_INSTANCE);
        return originTypes.stream().filter(originType -> originType.equalsIgnoreCase(type)).findFirst().orElse(type);
    }

    @Around("@within(org.springframework.stereotype.Controller) || @within(org.springframework.web.bind.annotation.RestController)")
    public Object around(ProceedingJoinPoint pjp) throws Throwable {
        Object[] args = pjp.getArgs();
        try {
            MethodSignature signature = (MethodSignature) pjp.getSignature();
            String[] parameterNames = signature.getParameterNames();
            Class[] parameterTypes = signature.getParameterTypes();
            for (int i = 0; i < parameterNames.length; i++) {
                if (args[i] == null) {
                    continue;
                }
                updateInsensitiveField(args, parameterNames, parameterTypes, i);
            }
        } catch (Exception e) {
            log.warn("update insensitive field failed ", e);
        }
        return pjp.proceed(args);
    }

    private void updateInsensitiveField(Object[] args, String[] parameterNames, Class[] parameterTypes, int i) {
        Object arg = args[i];
        if (parameterTypes[i] == String.class) {
            if (StringUtils.isEmpty((String) args[i])) {
                return;
            }
            switch (parameterNames[i]) {
            case "project":
                NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
                ProjectInstance projectInstance = projectManager.getProject((String) args[i]);
                if (projectInstance != null) {
                    args[i] = projectInstance.getName();
                }
                break;
            case "username":
                NKylinUserManager userManager = NKylinUserManager.getInstance(KylinConfig.getInstanceFromEnv());
                ManagedUser user = userManager.get((String) args[i]);
                if (user != null) {
                    args[i] = user.getUsername();
                }
                break;
            case "modelAlias":
                args[i] = getDataModelAlias(args, parameterNames, parameterTypes, (String) arg);
                break;
            case "type":
            case "sidType":
            case "entityType":
                args[i] = getCaseInsentiveType((String) arg);
                break;
            default:
                break;
            }

        } else if (arg instanceof InsensitiveRequest) {
            InsensitiveRequest insensitiveRequest = (InsensitiveRequest) arg;
            insensitiveRequest.updateField();
        }
    }

    /**
     * get project filed from parameters
     * @param args
     * @param parameterNames
     * @param parameterTypes
     * @param modelAlias
     * @return
     */
    private String getDataModelAlias(Object[] args, String[] parameterNames, Class[] parameterTypes,
            String modelAlias) {
        String projectName = null;

        for (int i = 0; i < parameterNames.length; i++) {
            if (parameterTypes[i] == String.class && Objects.equals("project", parameterNames[i])) {
                projectName = (String) args[i];
                NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
                ProjectInstance projectInstance = projectManager.getProject(projectName);
                if (projectInstance != null) {
                    projectName = projectInstance.getName();
                }
                break;
            }
        }
        if (StringUtils.isEmpty(projectName)) {
            return modelAlias;
        }
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), projectName);
        NDataModel dataModel = modelManager.getDataModelDescByAlias(modelAlias);
        if (dataModel != null) {
            return dataModel.getAlias();
        }
        return modelAlias;
    }
}
