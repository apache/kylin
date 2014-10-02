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
package com.kylinolap.rest.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

/**
 * Created with IntelliJ IDEA.
 * User: lukhan
 * Date: 9/2/13
 * Time: 2:34 PM
 * To change this template use File | Settings | File Templates.
 */
@ResponseStatus(value = HttpStatus.BAD_REQUEST)
public class BadRequestException extends RuntimeException {
    /**
     *
     */
    private static final long serialVersionUID = -6798154278095441848L;

    public BadRequestException(String s) {
        super(s);
    }

    /**
     *
     */
    public BadRequestException() {
        super();
    }

    /**
     * @param arg0
     * @param arg1
     */
    public BadRequestException(String arg0, Throwable arg1) {
        super(arg0, arg1);
    }

    /**
     * @param arg0
     */
    public BadRequestException(Throwable arg0) {
        super(arg0);
    }

}
