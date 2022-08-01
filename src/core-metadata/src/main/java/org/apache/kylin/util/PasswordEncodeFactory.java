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
package org.apache.kylin.util;

import static org.apache.kylin.common.exception.code.ErrorCodeSystem.PASSWORD_INIT_ENCODER_FAILED;
import static org.apache.kylin.common.exception.code.ErrorCodeSystem.PASSWORD_INVALID_ENCODER;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.springframework.security.crypto.password.PasswordEncoder;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PasswordEncodeFactory {
    public static PasswordEncoder newUserPasswordEncoder() {
        String userPasswordEncode = KylinConfig.getInstanceFromEnv().getUserPasswordEncoder();
        log.debug("using '{}' for user password encode", userPasswordEncode);
        PasswordEncoder passwordEncoder;
        try {
            passwordEncoder = (PasswordEncoder) Class.forName(userPasswordEncode).newInstance();
        } catch (ClassNotFoundException e) {
            log.error("class '{}' is not found, password init failed", userPasswordEncode, e);
            throw new KylinException(PASSWORD_INVALID_ENCODER);
        } catch (Exception e) {
            log.error("password encoder init failed", e);
            throw new KylinException(PASSWORD_INIT_ENCODER_FAILED);
        }
        return passwordEncoder;
    }

}
