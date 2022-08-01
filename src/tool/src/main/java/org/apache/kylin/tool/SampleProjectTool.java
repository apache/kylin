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

package org.apache.kylin.tool;

import static org.apache.kylin.common.exception.code.ErrorCodeServer.MODEL_NAME_DUPLICATE;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.ExecutableApplication;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.common.util.AddressUtil;
import org.apache.kylin.common.util.OptionBuilder;
import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.tool.util.ToolMainWrapper;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SampleProjectTool extends ExecutableApplication {

    private boolean checkProjectExist(String project) {
        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        ProjectInstance prjInstance = projectManager.getProject(project);
        return prjInstance != null;
    }

    private void assertModelNotExist(String project, String model) {
        NDataModel dataModel = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                .getDataModelDescByAlias(model);
        if (dataModel != null) {
            throw new KylinException(MODEL_NAME_DUPLICATE, model);
        }
    }

    private static final Option OPTION_PROJECT = OptionBuilder.getInstance().hasArg().withArgName("PROJECT_NAME")
            .isRequired(true).create("project");

    private static final Option OPTION_MODEL = OptionBuilder.getInstance().hasArg().withArgName("MODEL_NAME")
            .isRequired(true).create("model");

    private static final Option OPTION_DIR = OptionBuilder.getInstance().hasArg().withArgName("DIRECTORY_PATH")
            .isRequired(true).create("dir");

    @Override
    protected Options getOptions() {
        Options options = new Options();
        options.addOption(OPTION_PROJECT);
        options.addOption(OPTION_MODEL);
        options.addOption(OPTION_DIR);
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        String project = optionsHelper.getOptionValue(OPTION_PROJECT);
        String model = optionsHelper.getOptionValue(OPTION_MODEL);
        if (checkProjectExist(project)) {
            assertModelNotExist(project, model);
        }
        String dir = optionsHelper.getOptionValue(OPTION_DIR);
        val config = KylinConfig.getInstanceFromEnv();
        val resourceStore = ResourceStore.getKylinMetaStore(config);
        resourceStore.getAuditLogStore().setInstance(AddressUtil.getMockPortAddress());
        MetadataTool tool = new MetadataTool(config);
        tool.execute(new String[] { "-restore", "-dir", dir, "-project", project });
    }

    public static void main(String[] args) {
        ToolMainWrapper.wrap(args, () -> {
            SampleProjectTool tool = new SampleProjectTool();
            tool.execute(args);
        });
        Unsafe.systemExit(0);
    }
}
