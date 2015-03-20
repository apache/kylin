Kylin ODBC Driver Tutorial
===

> We provide Kylin ODBC driver to enable data access from ODBC-compatible client applications.

> Both 32-bit version or 64-bit version driver are available.

> Tested Operation System: Windows 7, Windows Server 2008 R2

> Tested Application: Tableau 8.0.4 and Tableau 8.1.3

## Prerequisites
1. Microsoft Visual C++ 2012 Redistributable 

   * For 32 bit Windows or 32 bit Tableau Desktop: Download: [32bit version](http://download.microsoft.com/download/1/6/B/16B06F60-3B20-4FF2-B699-5E9B7962F9AE/VSU_4/vcredist_x86.exe) 
   * For 64 bit Windows or 64 bit Tableau Desktop: Download: [64bit version](http://download.microsoft.com/download/1/6/B/16B06F60-3B20-4FF2-B699-5E9B7962F9AE/VSU_4/vcredist_x64.exe)

2. ODBC driver internally gets results from a REST server, make sure you have access to one

## Installation
1. Uninstall existing Kylin ODBC first, if you already installled it before
2. Download the attached driver installer at [KylinOLAP/odbc-driver/exe](https://github.com/KylinOLAP/odbc-driver/tree/master/exe), run it.

   * For 32 bit Tableau Desktop: Please install KylinODBCDriver (x86).exe
   * For 64 bit Tableau Desktop: Please install KylinODBCDriver (x64).exe

3. Both drivers already be installed on Tableau Server, you properly should be able to publish to there without issues

## Use ODBC compatible applications

1. [Kylin and Tableau Tutorial](Kylin and Tableau Tutorial.md)
2. [Kylin and Excel Tutorial](Kylin and Excel Tutorial.md)


## Bug Report
Open github issue here