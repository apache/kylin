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

package org.apache.kylin.dict;

class Util {
    
    /*
     * There are class names serialized into dict files. After apache package renaming, the old class names
     * requires adjustment in order to find the right class under new package.
     */
    static Class<?> classForName(String className) throws ClassNotFoundException {
        try {
            return Class.forName(className);
        } catch (ClassNotFoundException e) {
            
            // if package names were changed?
            int cut = className.lastIndexOf('.');
            String pkg = className.substring(0, cut);
            String newPkg = Util.class.getPackage().getName();
            if (pkg.equals(newPkg))
                throw e;
            
            // find again under new package
            String newClassName = newPkg + className.substring(cut);
            return Class.forName(newClassName);
        }
    }
}
