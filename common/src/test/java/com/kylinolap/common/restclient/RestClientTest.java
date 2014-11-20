package com.kylinolap.common.restclient;


import org.junit.Test;

import java.io.IOException;

public class RestClientTest {

    @Test
    public void basicTests() throws IOException {
        RestClient a = new RestClient("prod01:80");
        //a.wipeCache("metadata", "a", "a");
        //String aa = a.getKylinProperties();
        //System.out.println(aa);
        RestClient b = new RestClient("sandbox.hortonworks.com:7070");
        //b.wipeCache("metadata", "a", "a");
        //String bb = b.getKylinProperties();
        //System.out.println(bb);


    }

}