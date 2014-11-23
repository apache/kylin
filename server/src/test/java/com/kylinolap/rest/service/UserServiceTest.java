package com.kylinolap.rest.service;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;

public class UserServiceTest extends ServiceTestBase{

    @Autowired
    UserService userService;
    
    @Test
    public void testBasics(){
        userService.deleteUser("ADMIN");
        Assert.assertTrue(!userService.userExists("ADMIN"));
        
        List<GrantedAuthority> authorities = new ArrayList<GrantedAuthority>();
        authorities.add(new SimpleGrantedAuthority("ROLE_ADMIN"));
        User user = new User("ADMIN", "ADMIN", authorities);
        userService.createUser(user);
        
        Assert.assertTrue(userService.userExists("ADMIN"));
        
        UserDetails ud = userService.loadUserByUsername("ADMIN");
        Assert.assertTrue(ud.getUsername().equals("ADMIN"));
        
        Assert.assertTrue(userService.getUserAuthorities().size() > 0);
    }
}
