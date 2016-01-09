# About KylinODBC


ODBC Driver to interactive with Kylin REST server

The projects are organized as a Visual Studio 2012 solution. The entry of the solution is KylinODBC.sln. Mind the VS version.


## Contents in the folder


KylinODBC                  - Root folder containing the workspace. All the projects are part of this single workspace

KylinODBC\TestDll          - Contains a simple ODBC client that can be used to test your driver as well as connect to any ODBC data source. 

KylinODBC\Common           - Shared data types, utility tools, etc.

KylinODBC\Driver           - Contains code for Kylin ODBC driver. Note that the entire functionality has not been implemented but is enough to get you data into most standard ODBC clients like Tableau, provided you have set up a rest server to serve the query requests. Note that the header file is a very important starting point for understanding this driver.

KylinODBC\Installer  	   - Contains a MSI installer for kylin odbc driver(x86)

KylinODBC\Installer(64bit) - Contains a MSI installer for kylin odbc driver(x64)

# How to compile

## Development environment


* Microsoft Visual Studio 2012 or later
* Microsoft Windows SDK, if you're running .NET 4, install http://www.microsoft.com/en-us/download/details.aspx?id=8279, if you're running .NET 4.5, install https://msdn.microsoft.com/en-US/windows/desktop/hh852363.aspx
* Microsoft Data Access Components (MDAC, http://www.microsoft.com/en-us/download/details.aspx?id=21995), this is for getting the odbc header files
* Install InstallShield, this is for packing ODBC binaries into a windows installer in Visual Studio. Download it free: http://learn.flexerasoftware.com/content/IS-EVAL-InstallShield-Limited-Edition-Visual-Studio?lang=1033&ver=pro
* Install Visual Leak Detector, this is for detecting memory leaks for Visual C++ programs.Download at: http://vld.codeplex.com/releases


## Build zlib and Casablanca from source code

KylinODBC uses zlib for data compression and Casablanca as REST client. KylinODBC requires its libraries to be static libs, so we need to download source codes and compile it manually.  Remember if you want to build 64 bit odbc driver, you'll have to build 64 bit dependency libaries, the same goes for 32 bit. It is recommended that you build Release|x64, Debug|x64, Release|Win32, Debug|Win32 for all the libraries.

### Set ZLIB_HOME and CPPREST_HOME

Go to "Control Panel" -> "System" -> "Advanced system settings" -> "Environment Variables" -> "System variables", add two variables "ZLIB_HOME" and "CPPREST_HOME", their value being set to the home directory of zlib and Casablanca.

### Compile zlib

Download zlib 1.2.8 source code from its official website: http://www.zlib.net, refer to http://www.helyar.net/2010/compiling-zlib-lib-on-windows/ or http://blog.sina.com.cn/s/blog_6e0693f70100sjgj.html (this doc is in Chinese, but it's clearer) Notice that actually you only need to compile project "zlibstat"

Here're some tips when compiling zlib:
 	
 *  To avoid issue like http://stackoverflow.com/questions/20021950/def-file-syntax-error-in-visual-studio-2012, change "VERSION 1.2.8" in $ZLIB_HOME/zlibvc.def to "VERSION 1.2"
 *  Select All Configurations and All Platforms from the drop-downs at the top of the Properties dialog (right click project zlibstat), go to  Configuration Properties -> C/C++ -> Code Generation, change the Runtime Library option to Multi-threaded Debug (/MTd) for the Debug configuration and Multi-threaded (/MT) for the Release configuration.

### Compile Casablanca
		
We use Casablanca 2.0.1, like zlib, we need static libs of Casablanca, so again we need to compile it manually. Download 2.0.1 source code from code repository: http://casablanca.codeplex.com/SourceControl/changeset/fa40cc31af293417bb9f25d358a3af576226394a. Refer to https://katyscode.wordpress.com/2014/04/01/how-to-statically-link-the-c-rest-sdk-casablanca to build it.

### Compile KylinODBC

Open KylinODBC.sln, if you're building a 32 bit ODBC driver, set the profile to RELEASE|Win32, build project Installer and find the driver exe at Installer/Express/SingleImage/DiskImages/DISK1. If you're building a 64 bit driver, set the profile to RELEASE|x64, and find the exe at Installer(x64bit)/Express/SingleImage/DiskImages/DISK1.