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
package org.apache.kylin.rest.controller.v2;

import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V2_JSON;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.USER_AUTH_INFO_NOTFOUND;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.rest.controller.NBasicController;
import org.apache.kylin.rest.controller.NUserController;
import org.apache.kylin.rest.exception.UnauthorizedException;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ManagedUserResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import io.swagger.annotations.ApiOperation;
import lombok.val;

@Controller
@RequestMapping("/api")
public class NUserControllerV2 extends NBasicController {
    private static final Logger logger = LoggerFactory.getLogger(NUserControllerV2.class);

    @Autowired
    private NUserController nUserController;

    @ApiOperation(value = "listAllUsers", tags = { "MID" })
    @GetMapping(value = "/kap/user/users", produces = { HTTP_VND_APACHE_KYLIN_V2_JSON })
    @ResponseBody
    public EnvelopeResponse listAllUsers(@RequestParam(value = "name", required = false) String nameSeg,
            @RequestParam(value = "isCaseSensitive", required = false) boolean isCaseSensitive,
            @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer pageOffset,
            @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer pageSize)
            throws IOException {

        EnvelopeResponse<DataResult<List<ManagedUserResponse>>> dataResult = nUserController.listAllUsers(nameSeg,
                isCaseSensitive, pageOffset, pageSize);
        Map<String, Object> result = Maps.newHashMap();
        result.put("users", dataResult.getData().getValue());
        return new EnvelopeResponse<>(dataResult.getCode(), result, dataResult.getMsg());
    }

    @ApiOperation(value = "authenticate", tags = { "MID" })
    @PostMapping(value = "/user/authentication", produces = { HTTP_VND_APACHE_KYLIN_V2_JSON })
    @ResponseBody
    public EnvelopeResponse<UserDetails> authenticate() {
        EnvelopeResponse<UserDetails> response = authenticatedUser();
        logger.debug("User login: {}", response.getData());
        return response;
    }

    @ApiOperation(value = "authenticatedUser", tags = { "MID" })
    @GetMapping(value = "/user/authentication", produces = { HTTP_VND_APACHE_KYLIN_V2_JSON })
    @ResponseBody
    public EnvelopeResponse<UserDetails> authenticatedUser() {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        UserDetails data = null;
        val msg = MsgPicker.getMsg();
        if (authentication == null) {
            throw new UnauthorizedException(USER_AUTH_INFO_NOTFOUND);
        }

        if (authentication.getPrincipal() instanceof UserDetails) {
            data = (UserDetails) authentication.getPrincipal();
            return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, data, "");
        }

        if (authentication.getDetails() instanceof UserDetails) {
            data = (UserDetails) authentication.getDetails();
            return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, data, "");
        }

        throw new UnauthorizedException(USER_AUTH_INFO_NOTFOUND);
    }
}
