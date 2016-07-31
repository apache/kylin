package org.apache.kylin.rest.security;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;

public class PasswordPlaceHolderConfigurerTest {

    @Test
    public void testAESEncrypt(){
        String input = "hello world";
        String result = PasswordPlaceholderConfigurer.encrypt(input);
        Assert.assertEquals("4stv/RRleOtvie/8SLHmXA==", result);
    }

}
