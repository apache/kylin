package org.apache.kylin.rest.security;

import java.util.List;

import org.springframework.security.provisioning.UserDetailsManager;

public interface UserManager extends UserDetailsManager {

    public List<String> getUserAuthorities();

}
