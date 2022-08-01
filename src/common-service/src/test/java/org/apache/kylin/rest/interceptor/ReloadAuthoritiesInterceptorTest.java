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

package org.apache.kylin.rest.interceptor;

import java.net.ConnectException;
import java.util.Arrays;

import org.apache.kylin.common.AbstractTestCase;
import org.apache.kylin.rest.service.UserService;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.ldap.CommunicationException;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;

@ExtendWith({ MockitoExtension.class })
@MockitoSettings(strictness = Strictness.LENIENT)
@MetadataInfo(onlyProps = true)
public class ReloadAuthoritiesInterceptorTest extends AbstractTestCase {

    @InjectMocks
    private ReloadAuthoritiesInterceptor reloadAuthoritiesInterceptor = Mockito.spy(new ReloadAuthoritiesInterceptor());

    @Mock
    private UserService userService;

    @Test
    public void preHandle() throws Exception {

        overwriteSystemProp("kylin.env", "PROD");

        Authentication defaultAuthentication = SecurityContextHolder.getContext().getAuthentication();

        UsernamePasswordAuthenticationToken authenticationToken = new UsernamePasswordAuthenticationToken("ADMIN",
                "KYLIN", Arrays.asList(new SimpleGrantedAuthority("ROLE_USER")));

        SecurityContextHolder.getContext().setAuthentication(authenticationToken);

        MockHttpServletRequest request = new MockHttpServletRequest();

        MockHttpServletResponse response = new MockHttpServletResponse();

        javax.naming.CommunicationException communicationException = new javax.naming.CommunicationException();

        communicationException.setRootCause(new ConnectException());

        CommunicationException exception = new CommunicationException(communicationException);

        Mockito.when(userService.loadUserByUsername("ADMIN")).thenThrow(exception);

        Assertions.assertFalse(reloadAuthoritiesInterceptor.preHandle(request, response, null));

        SecurityContextHolder.getContext().setAuthentication(defaultAuthentication);

    }
}
