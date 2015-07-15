
Folder and their contents are listed. This will give you can an idea of what to find where. More details are available in the CPP files of the respective projects.

The projects have been built using Visual Studio 2012. The entry of the project is KylinODBC.sln. Mind the VS version.

Make sure you have the following header files, import libraries and DLLs on your system
1. SQL.H
2. SQLTYPES.H
3. SQLEXT.H
4. ODBCINST.H
5. ODBC32.LIB & ODBC32.DLL
6. ODBCCP32.LIB & ODBCCP32.DLL

You can obtain these from Microsoft site as a part of Platform SDK or MDAC kit


List of folders and description:
================================

KylinODBC                  - Root folder containing the workspace. All the projects are part of this single workspace

KylinODBC\TestDll         - Contains a simple ODBC client that can be used to test your driver as well as connect to any ODBC data source. 

KylinODBC\Common           - Shared data types, utiliy tools, etc.

KylinODBC\Driver           - Contains code for Kylin ODBC driver. Note that the entire functionality has not been implemented but is enough to get you data into most standard ODBC clients like Tableau, provided you have set up a rest server to serve the query requests. Note that the header file is a very important starting point for understanding this driver.

KylinODBC\Installer  	- Contains a MSI installer for kylin odbc driver(x86)

KylinODBC\Installer(64bit)  - Contains a MSI installer for kylin odbc driver(x64)

KylinODBC\doc               - Totorials for ODBC development starters.

How to compile
================================
1. Get a windows PC with Microsoft Visual Studio 2012 installed. Additionaly install Microsoft Data Access Components (MDAC, http://www.microsoft.com/en-us/download/details.aspx?id=21995) on it.

2. Download source code for zlib(for compression) and Casablanca(for REST invoking), and build them to static libaries. Remember if you want to build 64 bit odbc driver, you'll have to build 64 bit dependency libaries, the same goes for 32 bit 

		Compiling zlib: refer to http://www.helyar.net/2010/compiling-zlib-lib-on-windows/ or http://blog.sina.com.cn/s/blog_6e0693f70100sjgj.html(this doc is in Chineses, but it's quite clear)
		
		Compliling Casablanca: refer to https://katyscode.wordpress.com/2014/04/01/how-to-statically-link-the-c-rest-sdk-casablanca

3. Go to "Control Panel" -> "System" -> "Advanced system settings" -> "Environment Variables" -> "System variables", add two variables "ZLIB_HOME" and "CPPREST_HOME", their value being set to the home directory of zlib and Casablanca.

4. Open KylinODBC.sln, if you're building a 32 bit ODBC driver, set the profile to RELEASE|Win32, build project Installer and find the driver exe at Installer/Express/SingleImage/DiskImages/DISK1. If you're building a 64 bit driver, set the profile to RELEASE|x64, and find the exe at Installer(x64bit)/Express/SingleImage/DiskImages/DISK1.



all the best

Hongbin Ma
mahongbin@apache.org
