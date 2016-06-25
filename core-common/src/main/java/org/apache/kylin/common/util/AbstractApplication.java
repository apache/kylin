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

import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;

/**
 */
public abstract class AbstractApplication {

    protected abstract Options getOptions();

    protected abstract void execute(OptionsHelper optionsHelper) throws Exception;

    public final void execute(String[] args) {
        OptionsHelper optionsHelper = new OptionsHelper();
        System.out.println("Abstract Application args:" + StringUtils.join(args, " "));
        try {
            optionsHelper.parseOptions(getOptions(), args);
            execute(optionsHelper);
        } catch (ParseException e) {
            optionsHelper.printUsage(this.getClass().getName(), getOptions());
            throw new RuntimeException("error parsing args", e);
        } catch (Exception e) {
            throw new RuntimeException("error execute " + this.getClass().getName(), e);
        }
    }
}
