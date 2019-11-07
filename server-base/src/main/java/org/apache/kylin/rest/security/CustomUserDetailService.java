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

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.PostConstruct;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

@Service
public class CustomUserDetailService implements UserDetailsService {

    private static final Logger LOG = LoggerFactory.getLogger(CustomUserDetailService.class);

    private ConcurrentHashMap<String, CustomUser> userMap = new ConcurrentHashMap();

    private KylinConfig config;

    @PostConstruct
    public void init() {
        this.config = KylinConfig.getInstanceFromEnv();
        ScheduledExecutorService service = Executors.newScheduledThreadPool(1,
                new ThreadPoolExecutorThreadFactory("kylin-user-sync-"));
        service.scheduleWithFixedDelay(() -> {
            LOG.info("Start to load user-related permission config file");
            loadUser();
        }, 0, 2, TimeUnit.MINUTES);
    }

    @Override
    public UserDetails loadUserByUsername(String username) throws UsernameNotFoundException {

        if (StringUtils.isEmpty(username)) {
            throw new IllegalArgumentException(" loadUser is invalid when username is empty");
        }
        if (userMap.containsKey(username)) {
            CustomUser user = this.userMap.get(username);
            return new CustomUserDetail(user, getAuthorities(user));
        }
        return null;
    }

    private Collection<GrantedAuthority> getDefaultAuthorities() {
        Collection<GrantedAuthority> grantedAuthorities = new ArrayList();
        SimpleGrantedAuthority grantedAuthority = new SimpleGrantedAuthority("ROLE_ADMIN");
        grantedAuthorities.add(grantedAuthority);
        return grantedAuthorities;
    }

    private Collection<GrantedAuthority> getAuthorities(CustomUser user) {
        Collection<GrantedAuthority> grantedAuthorities = new ArrayList();
        List<String> userRole = user.getUserRole();

        if (CollectionUtils.isEmpty(userRole)) {
            return this.getDefaultAuthorities();
        }
        for (String role : userRole) {
            SimpleGrantedAuthority grantedAuthority = new SimpleGrantedAuthority(role);
            grantedAuthorities.add(grantedAuthority);
        }
        return grantedAuthorities;
    }

    public void loadUser() {

        String authFile = config.getAuthConfigFile();
        if (authFile.endsWith(".properties")) {
            loadConfigFromProperties(authFile);
        } else if (authFile.endsWith(".yaml")) {
            loadConfigFromYaml(authFile);
        } else {
            LOG.warn("Unsupported User Security Config File Format, {}", authFile);
        }
    }

    private void loadConfigFromProperties(String filename) {

        Properties props = this.loadProperties(filename);
        if (CollectionUtils.isEmpty(props)) {
            LOG.warn("User Security Config file {} is empty", filename);
            return;
        }
        ConcurrentHashMap<String, CustomUser> userMap = new ConcurrentHashMap<String, CustomUser>();
        for (String key : props.stringPropertyNames()) {
            CustomUser user = new CustomUser(key, props.getProperty(key));
            userMap.put(key, user);
        }
        this.userMap = userMap;
    }

    private void loadConfigFromYaml(String filename) {
        SecurityUsers users = this.loadYamlInternal(filename, SecurityUsers.class);
        List<CustomUser> allUsers = users.getUsers();
        if (CollectionUtils.isEmpty(allUsers)) {
            LOG.warn("User Auth-Info loaded from {} is empty", filename);
            return;
        }
        ConcurrentHashMap<String, CustomUser> userMap = new ConcurrentHashMap<>();
        for (CustomUser u : allUsers) {
            userMap.put(u.getUsername(), u);
        }
        this.userMap = userMap;
    }

    public Properties loadProperties(String filename) {
        Properties props = new Properties();
        try (InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(filename)) {
            BufferedInputStream bis = getConfigInputStream(filename, is);
            props.load(bis);
        } catch (IOException ioe) {
            throw new InternalErrorException("read " + filename + " error", ioe);
        }
        return props;
    }

    public <T> T loadYamlInternal(String filename, Class<T> o) {

        try (InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(filename)) {
            BufferedInputStream bis = getConfigInputStream(filename, is);
            return JsonUtil.readYamlValue(bis, o);
        } catch (IOException ioe) {
            throw new InternalErrorException("read " + filename + " error", ioe);
        }
    }

    private BufferedInputStream getConfigInputStream(String filename, InputStream is) throws FileNotFoundException {
        String propSrc = null;
        BufferedInputStream bis = null;
        if (is != null) {
            bis = new BufferedInputStream(is);
            propSrc = "the specified file : '" + filename + "' from the class resource path.";
        } else {
            bis = new BufferedInputStream(new FileInputStream(filename));
            propSrc = "the specified file : '" + filename + "'";
        }
        LOG.info("loading {}", propSrc);
        return bis;
    }

    public ConcurrentHashMap<String, CustomUser> getUserMap() {
        return userMap;
    }

    private static class ThreadPoolExecutorThreadFactory implements ThreadFactory {
        private final String name;
        private final AtomicInteger threadNumber = new AtomicInteger(1);

        private ThreadPoolExecutorThreadFactory(String name) {
            this.name = name;
        }

        @Override
        public Thread newThread(Runnable runnable) {
            Thread t = new Thread(runnable, name + threadNumber.getAndIncrement());
            t.setDaemon(true);
            return t;
        }
    }
}
