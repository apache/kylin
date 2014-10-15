package com.kylinolap.rest.security;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.provisioning.JdbcUserDetailsManager;

public class JdbcUserManager extends JdbcUserDetailsManager implements UserManager{

    @Autowired
    protected JdbcTemplate jdbcTemplate;

    public List<String> getUserAuthorities() {
        return jdbcTemplate.queryForList("select distinct authority from authorities", new String[] {}, String.class);
    }
    
}
