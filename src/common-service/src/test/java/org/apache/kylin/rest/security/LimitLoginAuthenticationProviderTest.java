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

import static org.apache.kylin.common.exception.code.ErrorCodeServer.USER_LOGIN_FAILED;

import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.user.ManagedUser;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.service.KylinUserService;
import org.apache.kylin.rest.service.UserAclService;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;
import org.junit.rules.ExpectedException;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.DisabledException;
import org.springframework.security.authentication.LockedException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.NoOpPasswordEncoder;
import org.springframework.security.crypto.password.Pbkdf2PasswordEncoder;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

public class LimitLoginAuthenticationProviderTest extends NLocalFileMetadataTestCase {

    @InjectMocks
    private LimitLoginAuthenticationProvider limitLoginAuthenticationProvider;

    @Mock
    private ServletRequestAttributes attrs;

    @Mock
    private UserAclService userAclService;

    @InjectMocks
    private KylinUserService userService;

    @InjectMocks
    @Spy
    private KylinUserService kylinUserService;

    private ManagedUser userAdmin = new ManagedUser("ADMIN", "KYLIN", false, Constant.ROLE_ADMIN);

    private ManagedUser userModeler = new ManagedUser("MODELER", "MODELER", false, Constant.ROLE_MODELER);

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Before
    public void setup() {
        createTestMetadata();
        MockitoAnnotations.initMocks(this);
        RequestContextHolder.setRequestAttributes(attrs);
        limitLoginAuthenticationProvider = Mockito.spy(new LimitLoginAuthenticationProvider());
        // spring security 5 has removed PlainTextPasswordEncoder
        // https://github.com/spring-projects/spring-security/blob/4.2.x/core/src/main/java/
        // org/springframework/security/authentication/encoding/PlaintextPasswordEncoder.java
        limitLoginAuthenticationProvider.setPasswordEncoder(NoOpPasswordEncoder.getInstance());
        ReflectionTestUtils.setField(limitLoginAuthenticationProvider, "userService", userService);
        ReflectionTestUtils.setField(limitLoginAuthenticationProvider, "userDetailsService", userService);
        kylinUserService.updateUser(userAdmin);
        kylinUserService.updateUser(userModeler);
    }

    @Test
    public void testAuthenticate_UserNotFound_EmptyUserName() {
        UsernamePasswordAuthenticationToken token = new UsernamePasswordAuthenticationToken("", userAdmin.getPassword(),
                userAdmin.getAuthorities());
        thrown.expect(BadCredentialsException.class);
        thrown.expectMessage(USER_LOGIN_FAILED.getMsg());
        limitLoginAuthenticationProvider.authenticate(token);
    }

    @Test
    public void testAuthenticate_UserNotFound_Exception() {
        UsernamePasswordAuthenticationToken token = new UsernamePasswordAuthenticationToken("lalala",
                userAdmin.getPassword(), userAdmin.getAuthorities());
        thrown.expect(BadCredentialsException.class);
        thrown.expectMessage(USER_LOGIN_FAILED.getMsg());
        limitLoginAuthenticationProvider.authenticate(token);
    }

    @Test
    public void testAuthenticate_InSensitiveCase() {
        UsernamePasswordAuthenticationToken token = new UsernamePasswordAuthenticationToken("admin", "KYLIN",
                userAdmin.getAuthorities());
        limitLoginAuthenticationProvider.authenticate(token);
    }

    @Test
    public void testAuthenticate_EmptyPassword() {
        UsernamePasswordAuthenticationToken token = new UsernamePasswordAuthenticationToken("ADMIN", "",
                userAdmin.getAuthorities());
        thrown.expect(BadCredentialsException.class);
        thrown.expectMessage(USER_LOGIN_FAILED.getMsg());
        limitLoginAuthenticationProvider.authenticate(token);
    }

    @Test
    public void testAuthenticate_WrongPWD_Exception() {
        UsernamePasswordAuthenticationToken token = new UsernamePasswordAuthenticationToken("ADMIN", "fff",
                userAdmin.getAuthorities());
        thrown.expect(BadCredentialsException.class);
        thrown.expectMessage(USER_LOGIN_FAILED.getMsg());
        limitLoginAuthenticationProvider.authenticate(token);
    }

    @Test
    public void testAuthenticate_Locked_Exception() {
        userAdmin.setLocked(true);
        userAdmin.setLockedTime(System.currentTimeMillis());
        userAdmin.setWrongTime(3);
        kylinUserService.updateUser(userAdmin);
        UsernamePasswordAuthenticationToken token = new UsernamePasswordAuthenticationToken("ADMIN", "KYLIN",
                userAdmin.getAuthorities());
        try {
            limitLoginAuthenticationProvider.authenticate(token);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof LockedException);
            String msg = e.getMessage();
            Assert.assertTrue(msg
                    .matches("For security concern, account ADMIN has been locked. Please try again in \\d+ seconds. "
                            + "Login failure again will be locked for 1 minutes.."));
        }
    }

    @Test
    public void testAuthenticate_Unlocked() {
        userAdmin.setLocked(true);
        userAdmin.setLockedTime(System.currentTimeMillis() - 60 * 1000);
        userAdmin.setWrongTime(3);
        kylinUserService.updateUser(userAdmin);
        UsernamePasswordAuthenticationToken token = new UsernamePasswordAuthenticationToken("ADMIN", "KYLIN",
                userAdmin.getAuthorities());
        try {
            limitLoginAuthenticationProvider.authenticate(token);
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testAuthenticate_Disabled_Exception() {
        userAdmin.setDisabled(true);
        kylinUserService.updateUser(userAdmin);
        UsernamePasswordAuthenticationToken token = new UsernamePasswordAuthenticationToken("ADMIN", "KYLIN",
                userAdmin.getAuthorities());
        thrown.expect(DisabledException.class);
        limitLoginAuthenticationProvider.authenticate(token);
    }

    @Test
    public void testPbkdf2PasswordEncoder() {
        limitLoginAuthenticationProvider.setPasswordEncoder(new Pbkdf2PasswordEncoder());
        UsernamePasswordAuthenticationToken token = new UsernamePasswordAuthenticationToken("ADMIN", "KYLIN",
                userAdmin.getAuthorities());
        thrown.expect(BadCredentialsException.class);
        limitLoginAuthenticationProvider.authenticate(token);
        limitLoginAuthenticationProvider.setPasswordEncoder(new BCryptPasswordEncoder());
    }

    @Test
    public void testBuildBadCredentialsException() {
        ThrowingRunnable func = () -> ReflectionTestUtils.invokeMethod(limitLoginAuthenticationProvider,
                "buildBadCredentialsException", "userName", new BadCredentialsException("test"));
        Assert.assertThrows(BadCredentialsException.class, func);
    }

    @Test
    public void testBuildLockedException() {
        Assert.assertThrows(LockedException.class, () -> ReflectionTestUtils
                .invokeMethod(limitLoginAuthenticationProvider, "buildLockedException", "userName"));
    }

}
