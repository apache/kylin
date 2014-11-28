/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kylinolap.rest.controller;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.mock.web.MockHttpServletRequest;

import com.kylinolap.rest.exception.BadRequestException;
import com.kylinolap.rest.exception.ForbiddenException;
import com.kylinolap.rest.exception.InternalErrorException;
import com.kylinolap.rest.exception.NotFoundException;
import com.kylinolap.rest.response.ErrorResponse;
import com.kylinolap.rest.service.ServiceTestBase;

/**
 * @author xduo
 * 
 */
public class BaseControllerTest extends ServiceTestBase {

    private BasicController basicController;

    @Before
    public void setup() {
        super.setUp();

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
        errorResponse = basicController.handleInternalError(request, internalErrorException);
        Assert.assertNotNull(errorResponse);

        BadRequestException badRequestException = new BadRequestException("error");
        errorResponse = basicController.handleBadRequest(request, badRequestException);
        Assert.assertNotNull(errorResponse);
    }
}
