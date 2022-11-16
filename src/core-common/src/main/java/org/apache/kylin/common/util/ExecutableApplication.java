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
package org.apache.kylin.common.util;

import static org.apache.kylin.common.exception.CommonErrorCode.FAILED_EXECUTE_SHELL;
import static org.apache.kylin.common.exception.CommonErrorCode.FAILED_PARSE_SHELL;

import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.kylin.common.exception.KylinException;

public abstract class ExecutableApplication implements Application {

    protected abstract Options getOptions();

    protected abstract void execute(OptionsHelper optionsHelper) throws Exception;

    public OptionsHelper convertToOptionsHelper(String[] args) {
        OptionsHelper optionsHelper = new OptionsHelper();
        try {
            optionsHelper.parseOptions(getOptions(), args);
        } catch (ParseException e) {
            optionsHelper.printUsage("error parsing args ", getOptions());
            throw new KylinException(FAILED_PARSE_SHELL, "error parsing args", e);
        }
        return optionsHelper;
    }

    public void execute(String[] args) {
        OptionsHelper optionsHelper = convertToOptionsHelper(args);
        try {
            execute(optionsHelper);
        } catch (Exception e) {
            throw new KylinException(FAILED_EXECUTE_SHELL, "error execute " + this.getClass().getName(), e);
        }
    }
}
