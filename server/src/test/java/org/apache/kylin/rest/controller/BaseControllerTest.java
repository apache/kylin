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

package org.apache.kylin.rest.controller;

import java.io.IOException;

import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.exception.ForbiddenException;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.exception.NotFoundException;
import org.apache.kylin.rest.response.ErrorResponse;
import org.apache.kylin.rest.service.ServiceTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.mock.web.MockHttpServletRequest;

/**
 * @author xduo
 */
public class BaseControllerTest extends ServiceTestBase {

    private BasicController basicController;

    @Before
    public void setup() throws Exception {
        super.setup();

        basicController = new BasicController();
    }

    @Test
    public void testBasics() throws IOException {
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setRequestURI("http://localhost");

        NotFoundException notFoundException = new NotFoundException("not found");
        ErrorResponse errorResponse = basicController.handleBadRequest(request, notFoundException);
        Assert.assertNotNull(errorResponse);

        ForbiddenException forbiddenException = new ForbiddenException("forbidden");
        errorResponse = basicController.handleForbidden(request, forbiddenException);
        Assert.assertNotNull(errorResponse);

        InternalErrorException internalErrorException = new InternalErrorException("error");
        errorResponse = basicController.handleError(request, internalErrorException);
        Assert.assertNotNull(errorResponse);

        BadRequestException badRequestException = new BadRequestException("error");
        errorResponse = basicController.handleBadRequest(request, badRequestException);
        Assert.assertNotNull(errorResponse);
    }
}
