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

package org.apache.kylin.tool.general;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.kylin.common.util.ExecutableApplication;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.common.util.EncryptUtil;
import org.apache.kylin.common.util.OptionBuilder;
import org.apache.kylin.common.util.Unsafe;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;

import lombok.val;

public class CryptTool extends ExecutableApplication {

    private final Option optionEncryptMethod;

    private final Option optionCharSequence;

    private final Options options;

    public CryptTool() {
        OptionBuilder optionBuilder = OptionBuilder.getInstance();
        optionBuilder.withArgName("ENCRYPT_METHOD");
        optionBuilder.hasArg();
        optionBuilder.withDescription("Specify the encrypt method: [AES, BCrypt]");
        optionBuilder.isRequired();
        optionBuilder.withLongOpt("encrypt-method");
        optionEncryptMethod = optionBuilder.create("e");

        optionBuilder.withArgName("CHAR_SEQUENCE");
        optionBuilder.hasArg();
        optionBuilder.withDescription("Specify the char sequence to be encrypted");
        optionBuilder.isRequired();
        optionBuilder.withLongOpt("char-sequence");
        optionCharSequence = optionBuilder.create("s");

        options = new Options();
        options.addOption(optionEncryptMethod);
        options.addOption(optionCharSequence);
    }

    public static void main(String[] args) {
        val tool = new CryptTool();
        tool.execute(args);
        Unsafe.systemExit(0);
    }

    @Override
    protected Options getOptions() {
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) {
        val encryptMethod = optionsHelper.getOptionValue(optionEncryptMethod);
        val passwordTxt = optionsHelper.getOptionValue(optionCharSequence);

        if ("AES".equalsIgnoreCase(encryptMethod)) {
            // for encrypt password like LDAP password
            System.out.println(EncryptUtil.encrypt(passwordTxt));
        } else if ("BCrypt".equalsIgnoreCase(encryptMethod)) {
            // for encrypt the predefined user password, like ADMIN, MODELER.
            BCryptPasswordEncoder bCryptPasswordEncoder = new BCryptPasswordEncoder();
            System.out.println(bCryptPasswordEncoder.encode(passwordTxt));
        } else {
            System.out.println("Unsupported encrypt method: " + encryptMethod);
            Unsafe.systemExit(1);
        }
    }
}
