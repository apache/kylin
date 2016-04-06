---
layout: docs15-cn
title:  Kylin ODBC 驱动程序教程
categories: 教程
permalink: /cn/docs15/tutorial/odbc.html
version: v1.2
since: v0.7.1
---

> 我们提供Kylin ODBC驱动程序以支持ODBC兼容客户端应用的数据访问。
> 
> 32位版本或64位版本的驱动程序都是可用的。
> 
> 测试操作系统：Windows 7，Windows Server 2008 R2
> 
> 测试应用：Tableau 8.0.4 和 Tableau 8.1.3

## 前提条件
1. Microsoft Visual C++ 2012 再分配（Redistributable）
   * 32位Windows或32位Tableau Desktop：下载：[32bit version](http://download.microsoft.com/download/1/6/B/16B06F60-3B20-4FF2-B699-5E9B7962F9AE/VSU_4/vcredist_x86.exe) 
   * 64位Windows或64位Tableau Desktop：下载：[64bit version](http://download.microsoft.com/download/1/6/B/16B06F60-3B20-4FF2-B699-5E9B7962F9AE/VSU_4/vcredist_x64.exe)

2. ODBC驱动程序内部从一个REST服务器获取结果，确保你能够访问一个

## 安装
1. 如果你已经安装，首先卸载已存在的Kylin ODBC
2. 从[下载](../../download/)下载附件驱动安装程序，并运行。
   * 32位Tableau Desktop：请安装KylinODBCDriver (x86).exe
   * 64位Tableau Desktop：请安装KylinODBCDriver (x64).exe

3. Both drivers already be installed on Tableau Server, you properly should be able to publish to there without issues

## 错误报告
如有问题，请报告错误至Apache Kylin JIRA，或者发送邮件到dev邮件列表。
