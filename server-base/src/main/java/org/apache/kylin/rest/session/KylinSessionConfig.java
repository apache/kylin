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

package org.apache.kylin.rest.session;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.session.ExpiringSession;
import org.springframework.session.SessionRepository;
import org.springframework.session.security.web.authentication.SpringSessionRememberMeServices;
import org.springframework.session.web.http.CookieHttpSessionStrategy;
import org.springframework.session.web.http.CookieSerializer;
import org.springframework.session.web.http.DefaultCookieSerializer;
import org.springframework.session.web.http.HttpSessionStrategy;
import org.springframework.session.web.http.MultiHttpSessionStrategy;
import org.springframework.session.web.http.SessionEventHttpSessionListenerAdapter;
import org.springframework.session.web.http.SessionRepositoryFilter;
import org.springframework.util.ClassUtils;
import org.springframework.util.ObjectUtils;

import javax.annotation.PostConstruct;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpSessionListener;
import java.util.ArrayList;
import java.util.List;

/**
 * fine tuning {@link org.springframework.session.config.annotation.web.http.SpringHttpSessionConfiguration
 * SpringHttpSessionConfiguration} to use {@link KylinSessionFilter} as session filter to substitute the origin
 * {@link SessionRepositoryFilter} which throws exception when no external {@link SessionRepository} bean is present.
 */
@Configuration
public class KylinSessionConfig {
    private CookieHttpSessionStrategy defaultHttpSessionStrategy = new CookieHttpSessionStrategy();

    private boolean usesSpringSessionRememberMeServices;

    private ServletContext servletContext;

    private CookieSerializer cookieSerializer;

    private HttpSessionStrategy httpSessionStrategy = this.defaultHttpSessionStrategy;

    private List<HttpSessionListener> httpSessionListeners = new ArrayList<HttpSessionListener>();

    @PostConstruct
    public void init() {
        if (this.cookieSerializer != null) {
            this.defaultHttpSessionStrategy.setCookieSerializer(this.cookieSerializer);
        } else if (this.usesSpringSessionRememberMeServices) {
            DefaultCookieSerializer cookieSerializer = new DefaultCookieSerializer();
            cookieSerializer.setRememberMeRequestAttribute(
                    SpringSessionRememberMeServices.REMEMBER_ME_LOGIN_ATTR);
            this.defaultHttpSessionStrategy.setCookieSerializer(cookieSerializer);
        }
    }

    @Bean
    public SessionEventHttpSessionListenerAdapter sessionEventHttpSessionListenerAdapter() {
        return new SessionEventHttpSessionListenerAdapter(this.httpSessionListeners);
    }

    @Bean
    public <S extends ExpiringSession> SessionRepositoryFilter<? extends ExpiringSession> springSessionRepositoryFilter(
            @Autowired(required = false) SessionRepository<S> sessionRepository) {
        SessionRepositoryFilter<S> sessionRepositoryFilter = new KylinSessionFilter<>(sessionRepository);
        sessionRepositoryFilter.setServletContext(this.servletContext);
        if (this.httpSessionStrategy instanceof MultiHttpSessionStrategy) {
            sessionRepositoryFilter.setHttpSessionStrategy((MultiHttpSessionStrategy) this.httpSessionStrategy);
        } else {
            sessionRepositoryFilter.setHttpSessionStrategy(this.httpSessionStrategy);
        }
        return sessionRepositoryFilter;
    }

    public void setApplicationContext(ApplicationContext applicationContext)
            throws BeansException {
        if (ClassUtils.isPresent("org.springframework.security.web.authentication.RememberMeServices", null)) {
            this.usesSpringSessionRememberMeServices = !ObjectUtils.isEmpty(
                    applicationContext.getBeanNamesForType(SpringSessionRememberMeServices.class));
        }
    }

    @Autowired(required = false)
    public void setServletContext(ServletContext servletContext) {
        this.servletContext = servletContext;
    }

    @Autowired(required = false)
    public void setCookieSerializer(CookieSerializer cookieSerializer) {
        this.cookieSerializer = cookieSerializer;
    }

    @Autowired(required = false)
    public void setHttpSessionStrategy(HttpSessionStrategy httpSessionStrategy) {
        this.httpSessionStrategy = httpSessionStrategy;
    }

    @Autowired(required = false)
    public void setHttpSessionListeners(List<HttpSessionListener> listeners) {
        this.httpSessionListeners = listeners;
    }
}
