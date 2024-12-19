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

package org.apache.kylin.common.msg;

public class MsgPicker {
    private static final ThreadLocal<Message> msg = new ThreadLocal<>();
    private static final String CHINESE_LANGUAGE_CODE = "cn";

    /**
     * Sets the message based on the given language code.
     * If the language code is null or not recognized, sets the default English message.
     *
     * @param lang the language code (e.g., "cn" for Chinese)
     */
    public static void setMsg(String lang) {
        if (CHINESE_LANGUAGE_CODE.equals(lang)) {
            msg.set(CnMessage.getInstance());
        } else {
            msg.set(Message.getInstance());
        }
    }

    /**
     * Gets the current message. If no message is set, returns the default English message.
     *
     * @return the current message or the default English message if none is set
     */
    public static Message getMsg() {
        Message ret = msg.get();
        if (ret == null) {
            ret = Message.getInstance();
            msg.set(ret);
        }
        return ret;
    }
}
