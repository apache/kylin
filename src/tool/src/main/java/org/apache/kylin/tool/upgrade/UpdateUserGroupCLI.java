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
package org.apache.kylin.tool.upgrade;

import static org.apache.kylin.common.exception.ServerErrorCode.USERGROUP_NOT_EXIST;
import static org.apache.kylin.common.persistence.ResourceStore.USER_GROUP_ROOT;
import static org.apache.kylin.tool.util.MetadataUtil.getMetadataUrl;
import static org.apache.kylin.tool.util.ScreenPrintUtil.printlnGreen;
import static org.apache.kylin.tool.util.ScreenPrintUtil.systemExitWhenMainThread;

import java.io.File;
import java.util.List;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.ExecutableApplication;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.common.util.OptionBuilder;
import org.apache.kylin.metadata.usergroup.UserGroup;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/**
 * 4.1 -> 4.2
 */
@Slf4j
public class UpdateUserGroupCLI extends ExecutableApplication {

    private static final Option OPTION_METADATA_DIR = OptionBuilder.getInstance().hasArg().withArgName("metadata_dir")
            .withDescription("metadata dir.").isRequired(true).withLongOpt("metadata_dir").create("d");

    private static final Option OPTION_EXEC = OptionBuilder.getInstance().hasArg(false).withArgName("exec")
            .withDescription("exec the upgrade.").isRequired(false).withLongOpt("exec").create("e");

    public static void main(String[] args) {
        log.info("Start upgrade user group metadata.");
        try {
            UpdateUserGroupCLI tool = new UpdateUserGroupCLI();
            tool.execute(args);
        } catch (Exception e) {
            log.error("Upgrade user group metadata failed.", e);
            systemExitWhenMainThread(1);
        }
        log.info("Upgrade user group metadata successfully.");
        systemExitWhenMainThread(0);
    }

    @Override
    protected Options getOptions() {
        Options options = new Options();
        options.addOption(OPTION_METADATA_DIR);
        options.addOption(OPTION_EXEC);
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        String metadataUrl = getMetadataUrl(optionsHelper.getOptionValue(OPTION_METADATA_DIR));
        Preconditions.checkArgument(StringUtils.isNotBlank(metadataUrl));
        File userGroupFile = new File(metadataUrl, USER_GROUP_ROOT);
        if (!userGroupFile.exists()) {
            throw new KylinException(USERGROUP_NOT_EXIST, "User group metadata doesn't exist.");
        }
        if (userGroupFile.isDirectory()) {
            printlnGreen("user group metadata upgrade succeeded.");
            log.info("Succeed to upgrade user group metadata.");
            return;
        }
        printlnGreen("found user group metadata need to be upgraded.");
        if (optionsHelper.hasOption(OPTION_EXEC)) {
            UserGroup4Dot1 userGroups = JsonUtil.readValue(userGroupFile, UserGroup4Dot1.class);
            FileUtils.forceDelete(userGroupFile);
            userGroupFile.mkdirs();
            for (String groupName : userGroups.getGroups()) {
                File out = new File(userGroupFile, groupName);
                UserGroup userGroup = new UserGroup(groupName);
                JsonUtil.writeValue(out, userGroup);
            }
            printlnGreen("user group metadata upgrade succeeded.");
            log.info("Succeed to upgrade user group metadata.");
        }
    }

    @Data
    private static class UserGroup4Dot1 {
        @JsonProperty("groups")
        List<String> groups;
    }

}
