# Microsoft Developer Studio Project File - Name="driver" - Package Owner=<4>
# Microsoft Developer Studio Generated Build File, Format Version 6.00
# ** DO NOT EDIT **

# TARGTYPE "Win32 (x86) Dynamic-Link Library" 0x0102

CFG=driver - Win32 Debug
!MESSAGE This is not a valid makefile. To build this project using NMAKE,
!MESSAGE use the Export Makefile command and run
!MESSAGE 
!MESSAGE NMAKE /f "driver.mak".
!MESSAGE 
!MESSAGE You can specify a configuration when running NMAKE
!MESSAGE by defining the macro CFG on the command line. For example:
!MESSAGE 
!MESSAGE NMAKE /f "driver.mak" CFG="driver - Win32 Debug"
!MESSAGE 
!MESSAGE Possible choices for configuration are:
!MESSAGE 
!MESSAGE "driver - Win32 Release" (based on "Win32 (x86) Dynamic-Link Library")
!MESSAGE "driver - Win32 Debug" (based on "Win32 (x86) Dynamic-Link Library")
!MESSAGE 

# Begin Project
# PROP AllowPerConfigDependencies 0
# PROP Scc_ProjName ""
# PROP Scc_LocalPath ""
CPP=cl.exe
MTL=midl.exe
RSC=rc.exe

!IF  "$(CFG)" == "driver - Win32 Release"

# PROP BASE Use_MFC 0
# PROP BASE Use_Debug_Libraries 0
# PROP BASE Output_Dir "Release"
# PROP BASE Intermediate_Dir "Release"
# PROP BASE Target_Dir ""
# PROP Use_MFC 0
# PROP Use_Debug_Libraries 0
# PROP Output_Dir "Release"
# PROP Intermediate_Dir "Release"
# PROP Ignore_Export_Lib 0
# PROP Target_Dir ""
# ADD BASE CPP /nologo /MT /W3 /GX /O2 /D "WIN32" /D "NDEBUG" /D "_WINDOWS" /D "_MBCS" /D "_USRDLL" /D "driver_EXPORTS" /YX /FD /c
# ADD CPP /nologo /MT /W3 /GX /O2 /I "." /I "..\common" /I "..\common\xml" /I "..\common\sock" /D "NDEBUG" /D "WIN32" /D "_WINDOWS" /D "_MBCS" /D "_USRDLL" /D "driver_EXPORTS" /D "_XML_STREAM_SOCK_CLIENT" /YX /FD /c
# ADD BASE MTL /nologo /D "NDEBUG" /mktyplib203 /win32
# ADD MTL /nologo /D "NDEBUG" /mktyplib203 /win32
# ADD BASE RSC /l 0x409 /d "NDEBUG"
# ADD RSC /l 0x409 /d "NDEBUG"
BSC32=bscmake.exe
# ADD BASE BSC32 /nologo
# ADD BSC32 /nologo
LINK32=link.exe
# ADD BASE LINK32 kernel32.lib user32.lib gdi32.lib winspool.lib comdlg32.lib advapi32.lib shell32.lib ole32.lib oleaut32.lib uuid.lib odbc32.lib odbccp32.lib /nologo /dll /machine:I386
# ADD LINK32 odbccp32.lib kernel32.lib user32.lib gdi32.lib advapi32.lib ole32.lib oleaut32.lib uuid.lib odbc32.lib mswsock.lib ws2_32.lib /nologo /dll /machine:I386

!ELSEIF  "$(CFG)" == "driver - Win32 Debug"

# PROP BASE Use_MFC 0
# PROP BASE Use_Debug_Libraries 1
# PROP BASE Output_Dir "Debug"
# PROP BASE Intermediate_Dir "Debug"
# PROP BASE Target_Dir ""
# PROP Use_MFC 0
# PROP Use_Debug_Libraries 1
# PROP Output_Dir ""
# PROP Intermediate_Dir "Debug"
# PROP Ignore_Export_Lib 0
# PROP Target_Dir ""
# ADD BASE CPP /nologo /MTd /W3 /Gm /GX /ZI /Od /D "WIN32" /D "_DEBUG" /D "_WINDOWS" /D "_MBCS" /D "_USRDLL" /D "driver_EXPORTS" /YX /FD /GZ /c
# ADD CPP /nologo /MTd /W3 /Gm /GX /ZI /Od /I "." /I "..\common" /I "..\common\xml" /I "..\common\sock" /D "_DEBUG" /D "WIN32" /D "_WINDOWS" /D "_MBCS" /D "_USRDLL" /D "driver_EXPORTS" /D "_XML_STREAM_SOCK_CLIENT" /YX /FD /GZ /c
# ADD BASE MTL /nologo /D "_DEBUG" /mktyplib203 /win32
# ADD MTL /nologo /D "_DEBUG" /mktyplib203 /win32
# ADD BASE RSC /l 0x409 /d "_DEBUG"
# ADD RSC /l 0x409 /d "_DEBUG"
BSC32=bscmake.exe
# ADD BASE BSC32 /nologo
# ADD BSC32 /nologo
LINK32=link.exe
# ADD BASE LINK32 kernel32.lib user32.lib gdi32.lib winspool.lib comdlg32.lib advapi32.lib shell32.lib ole32.lib oleaut32.lib uuid.lib odbc32.lib odbccp32.lib /nologo /dll /debug /machine:I386 /pdbtype:sept
# ADD LINK32 kernel32.lib user32.lib gdi32.lib advapi32.lib ole32.lib oleaut32.lib uuid.lib odbc32.lib odbccp32.lib mswsock.lib ws2_32.lib /nologo /dll /incremental:no /debug /machine:I386 /pdbtype:sept

!ENDIF 

# Begin Target

# Name "driver - Win32 Release"
# Name "driver - Win32 Debug"
# Begin Group "Source Files"

# PROP Default_Filter "cpp;c;cxx;rc;def;r;odl;idl;hpj;bat"
# Begin Source File

SOURCE=..\Common\common.cpp
# End Source File
# Begin Source File

SOURCE=.\GO_ALLOC.CPP
# End Source File
# Begin Source File

SOURCE=.\GO_ATTR.CPP
# End Source File
# Begin Source File

SOURCE=.\GO_CONN.CPP
# End Source File
# Begin Source File

SOURCE=.\GO_CTLG.CPP
# End Source File
# Begin Source File

SOURCE=.\GO_DESC.CPP
# End Source File
# Begin Source File

SOURCE=.\GO_DIAG.CPP
# End Source File
# Begin Source File

SOURCE=.\GO_DTYPE.CPP
# End Source File
# Begin Source File

SOURCE=.\GO_EXEC.CPP
# End Source File
# Begin Source File

SOURCE=.\GO_FETCH.CPP
# End Source File
# Begin Source File

SOURCE=.\GO_INFO.CPP
# End Source File
# Begin Source File

SOURCE=.\GO_PARAM.CPP
# End Source File
# Begin Source File

SOURCE=.\GO_SOAP.CPP
# End Source File
# Begin Source File

SOURCE=.\GO_UTILS.CPP
# End Source File
# Begin Source File

SOURCE=.\GODBC32.CPP
# End Source File
# Begin Source File

SOURCE=.\driver.DEF
# End Source File
# Begin Source File

SOURCE=..\Common\SOCK\SOCK_CLI.CPP
# End Source File
# Begin Source File

SOURCE=..\Common\XML\XMLLEX.cpp
# End Source File
# Begin Source File

SOURCE=..\Common\XML\XMLNLST.cpp
# End Source File
# Begin Source File

SOURCE=..\Common\XML\XMLPARSE.cpp
# End Source File
# Begin Source File

SOURCE=..\Common\XML\XMLPHELP.cpp
# End Source File
# Begin Source File

SOURCE=..\Common\XML\XMLTREE.cpp
# End Source File
# End Group
# Begin Group "Header Files"

# PROP Default_Filter "h;hpp;hxx;hm;inl"
# Begin Source File

SOURCE=..\Common\char.h
# End Source File
# Begin Source File

SOURCE=..\Common\common.hpp
# End Source File
# Begin Source File

SOURCE=.\GODBC.H
# End Source File
# Begin Source File

SOURCE=.\resource.h
# End Source File
# Begin Source File

SOURCE=..\Common\SOCK\SOCK_CLI.HPP
# End Source File
# Begin Source File

SOURCE=..\Common\SOCK\SOCK_CLI.HXX
# End Source File
# Begin Source File

SOURCE=..\Common\types.h
# End Source File
# Begin Source File

SOURCE=..\Common\XML\xmllex.hpp
# End Source File
# Begin Source File

SOURCE=..\Common\XML\XMLPARSE.H
# End Source File
# Begin Source File

SOURCE=..\Common\XML\xmlparse.hpp
# End Source File
# Begin Source File

SOURCE=..\Common\XML\xmlparse.hxx
# End Source File
# Begin Source File

SOURCE=..\Common\XML\xmltree.hpp
# End Source File
# Begin Source File

SOURCE=..\Common\XML\xmltree.hxx
# End Source File
# End Group
# Begin Group "Resource Files"

# PROP Default_Filter "ico;cur;bmp;dlg;rc2;rct;bin;rgs;gif;jpg;jpeg;jpe"
# Begin Source File

SOURCE=.\GODBC.RC
# End Source File
# End Group
# End Target
# End Project
