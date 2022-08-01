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

package org.apache.kylin.rest.security;

import java.io.IOException;
import java.net.ConnectException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletResponse;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mockito;
import org.springframework.ldap.CommunicationException;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.security.authentication.DisabledException;
import org.springframework.security.authentication.InsufficientAuthenticationException;
import org.springframework.security.authentication.LockedException;
import org.springframework.security.core.AuthenticationException;

public class NUnauthorisedEntryPointTest {

    @InjectMocks
    private NUnauthorisedEntryPoint nUnauthorisedEntryPoint = Mockito.spy(new NUnauthorisedEntryPoint());

    @Test
    public void commence() throws ServletException, IOException {

        javax.naming.CommunicationException communicationException = new javax.naming.CommunicationException();
        communicationException.setRootCause(new ConnectException());
        CommunicationException exception = new CommunicationException(communicationException);

        MockHttpServletResponse response = new MockHttpServletResponse();

        nUnauthorisedEntryPoint.commence(new MockHttpServletRequest(), response,
                new AuthenticationException("", exception) {
                });
        Assertions.assertEquals(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, response.getStatus());

        response = new MockHttpServletResponse();
        nUnauthorisedEntryPoint.commence(new MockHttpServletRequest(), response, new AuthenticationException("") {
        });
        Assertions.assertEquals(HttpServletResponse.SC_UNAUTHORIZED, response.getStatus());

        response = new MockHttpServletResponse();
        nUnauthorisedEntryPoint.commence(new MockHttpServletRequest(), response, new LockedException(""));
        Assertions.assertEquals(HttpServletResponse.SC_BAD_REQUEST, response.getStatus());

        response = new MockHttpServletResponse();
        nUnauthorisedEntryPoint.commence(new MockHttpServletRequest(), response,
                new InsufficientAuthenticationException(""));
        Assertions.assertEquals(HttpServletResponse.SC_UNAUTHORIZED, response.getStatus());

        response = new MockHttpServletResponse();
        nUnauthorisedEntryPoint.commence(new MockHttpServletRequest(), response, new DisabledException(""));
        Assertions.assertEquals(HttpServletResponse.SC_UNAUTHORIZED, response.getStatus());

        response = new MockHttpServletResponse();
        nUnauthorisedEntryPoint.commence(new MockHttpServletRequest(), response, new AuthenticationException("",
                new org.springframework.ldap.AuthenticationException(new javax.naming.AuthenticationException())) {
        });
        Assertions.assertEquals(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, response.getStatus());

    }
}
