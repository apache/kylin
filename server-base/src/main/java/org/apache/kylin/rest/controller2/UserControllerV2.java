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

package org.apache.kylin.rest.controller2;

import java.io.IOException;

import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.service.UserService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * Handle user authentication request to protected kylin rest resources by
 * spring security.
 * 
 * @author xduo
 * 
 */
@Controller
@RequestMapping(value = "/user")
public class UserControllerV2 extends BasicController {

    private static final Logger logger = LoggerFactory.getLogger(UserControllerV2.class);

    @Autowired
    @Qualifier("userService")
    UserService userService;

    @RequestMapping(value = "/authentication", method = RequestMethod.POST, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse authenticateV2(@RequestHeader("Accept-Language") String lang) {
        EnvelopeResponse response = authenticatedUserV2(lang);
        logger.debug("User login: {}", response.data);
        return response;
    }

    @RequestMapping(value = "/authentication", method = RequestMethod.GET, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse authenticatedUserV2(@RequestHeader("Accept-Language") String lang) {
        MsgPicker.setMsg(lang);
        Message msg = MsgPicker.getMsg();

        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        UserDetails data = null;

        if (authentication == null) {
            logger.debug("authentication is null.");
            throw new BadRequestException(msg.getAUTH_INFO_NOT_FOUND());
        }

        if (authentication.getPrincipal() instanceof UserDetails) {
            data = (UserDetails) authentication.getPrincipal();
            return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, data, "");
        }

        if (authentication.getDetails() instanceof UserDetails) {
            data = (UserDetails) authentication.getDetails();
            return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, data, "");
        }

        throw new BadRequestException(msg.getAUTH_INFO_NOT_FOUND());
    }

    @RequestMapping(value = "/authentication/authorities", method = RequestMethod.GET, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getAuthoritiesV2(@RequestHeader("Accept-Language") String lang) throws IOException {
        MsgPicker.setMsg(lang);

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, userService.listUserAuthorities(), "");
    }

}
