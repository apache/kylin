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
