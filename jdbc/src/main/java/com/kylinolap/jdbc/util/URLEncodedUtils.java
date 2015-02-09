/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kylinolap.jdbc.util;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Properties;
import java.util.StringTokenizer;

import com.kylinolap.jdbc.Driver;

/**
 * Util class to parse connectionURL parameters.
 * 
 * @author R.C
 * 
 */

public class URLEncodedUtils {
    
    public final static String URLPARAMS_CHARACTER_ENCODING = "characterEncoding";
    
    /**
     * parse connectionURL parameters
     * @param strUrl jdbc:kylin://host/project[?characterEncoding=UTF-8[&..key1=value1]]
     * 
     * @param info 
     **/
    public static String parse(String strUrl,Properties info) {
              
        String url = strUrl.replace(Driver.CONNECT_STRING_PREFIX + "//", "");
        
        int idx = url.indexOf("?");
        if ( idx != -1){
            
            String keyValues = url.substring(idx + 1);
            url = url.substring(0, idx);
            
            StringTokenizer queryParams = new StringTokenizer(keyValues, "&"); 
            while (queryParams.hasMoreTokens()) {
                
                String parameterValuePair = queryParams.nextToken();

                int indexOfEquals = parameterValuePair.indexOf("=");

                String parameter = null;
                String value = null;

                if (indexOfEquals != -1) {
                    parameter = parameterValuePair.substring(0, indexOfEquals);

                    if (indexOfEquals + 1 < parameterValuePair.length()) {
                        value = parameterValuePair.substring(indexOfEquals + 1);
                    }
                }

                if ((value != null && value.length() > 0)
                        && (parameter != null && parameter.length() > 0)) {
                    try {
                        info.put(parameter, URLDecoder.decode(value,
                                "UTF-8"));
                    } catch (UnsupportedEncodingException badEncoding) {
                        // punt
                        info.put(parameter, URLDecoder.decode(value));
                    } catch (NoSuchMethodError nsme) {
                        // punt again
                        info.put(parameter, URLDecoder.decode(value));
                    }
                }
            }
        }
        
        return url;
    } 
}
