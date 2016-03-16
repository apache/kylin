---
layout: docs15
title:  Kylin ODBC Driver
categories: tutorial
permalink: /docs15/tutorial/odbc.html
since: v0.7.1
---

> We provide Kylin ODBC driver to enable data access from ODBC-compatible client applications.
> 
> Both 32-bit version or 64-bit version driver are available.
> 
> Tested Operation System: Windows 7, Windows Server 2008 R2
> 
> Tested Application: Tableau 8.0.4, Tableau 8.1.3 and Tableau 9.1

## Prerequisites
1. Microsoft Visual C++ 2012 Redistributable 
   * For 32 bit Windows or 32 bit Tableau Desktop: Download: [32bit version](http://download.microsoft.com/download/1/6/B/16B06F60-3B20-4FF2-B699-5E9B7962F9AE/VSU_4/vcredist_x86.exe) 
   * For 64 bit Windows or 64 bit Tableau Desktop: Download: [64bit version](http://download.microsoft.com/download/1/6/B/16B06F60-3B20-4FF2-B699-5E9B7962F9AE/VSU_4/vcredist_x64.exe)


2. ODBC driver internally gets results from a REST server, make sure you have access to one

## Installation
1. Uninstall existing Kylin ODBC first, if you already installled it before
2. Download ODBC Driver from [download](../../download/).
   * For 32 bit Tableau Desktop: Please install KylinODBCDriver (x86).exe
   * For 64 bit Tableau Desktop: Please install KylinODBCDriver (x64).exe

3. Both drivers already be installed on Tableau Server, you properly should be able to publish to there without issues

## DSN configuration
1. Open ODBCAD to configure DSN.
	* For 32 bit driver, please use the 32bit version in C:\Windows\SysWOW64\odbcad32.exe
	* For 64 bit driver, please use the default "Data Sources (ODBC)" in Control Panel/Administrator Tools
![]( /images/Kylin-ODBC-DSN/1.png)

2. Open "System DSN" tab, and click "Add", you will see KylinODBCDriver listed as an option, Click "Finish" to continue.
![]( /images/Kylin-ODBC-DSN/2.png)

3. In the pop up dialog, fill in all the blanks, The server host is where your Kylin Rest Server is started.
![]( /images/Kylin-ODBC-DSN/3.png)

4. Click "Done", and you will see your new DSN listed in the "System Data Sources", you can use this DSN afterwards.
![]( /images/Kylin-ODBC-DSN/4.png)

## Bug Report
Please open Apache Kylin JIRA to report bug, or send to dev mailing list.
