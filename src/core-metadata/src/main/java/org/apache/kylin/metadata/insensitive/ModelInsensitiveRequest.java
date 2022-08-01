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

package org.apache.kylin.metadata.insensitive;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.project.NProjectManager;

import lombok.extern.slf4j.Slf4j;

public interface ModelInsensitiveRequest extends InsensitiveRequest {

    default List<String> inSensitiveFields() {
        return Collections.singletonList("alias");
    }

    @Slf4j
    final class LogHolder {
    }

    @Override
    default void updateField() {
        for (String fieldName : inSensitiveFields()) {
            Field field = getDeclaredField(this.getClass(), fieldName);
            if (field == null) {
                return;
            }
            Unsafe.changeAccessibleObject(field, true);
            try {
                String modelAlias = (String) field.get(this);
                if (StringUtils.isEmpty(modelAlias)) {
                    continue;
                }
                // get project filed from object
                Field projectField = getDeclaredField(this.getClass(), "project");

                if (projectField == null) {
                    continue;
                }
                Unsafe.changeAccessibleObject(projectField, true);
                String projectName = (String) projectField.get(this);

                if (StringUtils.isEmpty(projectName)) {
                    continue;
                }

                NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
                ProjectInstance projectInstance = projectManager.getProject(projectName);
                if (projectInstance == null) {
                    continue;
                }

                projectName = projectInstance.getName();

                projectField.set(this, projectName);

                modelAlias = getDataModelAlias(modelAlias, projectName);

                field.set(this, modelAlias);
            } catch (IllegalAccessException e) {
                LogHolder.log.warn("update model name failed ", e);
            }
        }
    }

    default String getDataModelAlias(String modelAlias, String projectName) {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), projectName);
        NDataModel dataModel = modelManager.getDataModelDescByAlias(modelAlias);
        if (dataModel != null) {
            return dataModel.getAlias();
        }
        return modelAlias;
    }
}
