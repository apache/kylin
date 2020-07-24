<!--
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
-->
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN"
        "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">

<head>
    <meta http-equiv="Content-Type" content="Multipart/Alternative; charset=UTF-8"/>
    <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
</head>

<style>
    html {
        font-size: 10px;
    }

    * {
        box-sizing: border-box;
    }

    a:hover,
    a:focus {
        color: #23527c;
        text-decoration: underline;
    }

    a:focus {
        outline: 5px auto -webkit-focus-ring-color;
        outline-offset: -2px;
    }
</style>

<body>
<div style="font-family: 'Trebuchet MS ', Arial, Helvetica, sans-serif;">
        <span style="
line-height: 1;font-size: 16px;">
            <p style="text-align:left;">Dear ${requester},</p>
            <p>Your cube migration request has completed. Please check the cube in Kylin production.</p>
        </span>
    <hr style="margin-top: 10px;
margin-bottom: 10px;
height:0px;
border-top: 1px solid #eee;
border-right:0px;
border-bottom:0px;
border-left:0px;">
    <span style="display: inline;
                background-color: #5cb85c;
                color: #fff;
                line-height: 1;
                font-weight: 700;
                font-size:36px;
                text-align: center;">&nbsp;Completed&nbsp;</span>
    <hr style="margin-top: 10px;
margin-bottom: 10px;
height:0px;
border-top: 1px solid #eee;
border-right:0px;
border-bottom:0px;
border-left:0px;">

    <table cellpadding="0" cellspacing="0" width="100%" style="border-collapse: collapse;border:1px solid #d6e9c6;">

        <tr>

            <td style="padding: 10px 15px;
            background-color: #dff0d8;
            border:1px solid #d6e9c6;">
                <h4 style="margin-top: 0;
                margin-bottom: 0;
                font-size: 14px;
                color: #3c763d;
                font-family: 'Trebuchet MS ', Arial, Helvetica, sans-serif;">
                    Request detail
                </h4>
            </td>
        </tr>
        <tr>

            <td style="padding: 15px;">
                <table cellpadding="0" cellspacing="0" width="100%"
                       style="margin-bottom: 20px;border:1 solid #ddd;border-collapse: collapse;font-family: 'Trebuchet MS ', Arial, Helvetica, sans-serif;">
                    <tr>
                        <th width="30%" style="border: 1px solid #ddd;
                                padding: 8px;">
                            <h4 style="
                                        margin-top: 0;
                                        margin-bottom: 0;
                                        line-height: 1.5;
                                        text-align: left;
                                        font-size: 14px;
                                        font-style: normal;">Requester</h4>
                        </th>
                        <td style="border: 1px solid #ddd;
                            padding: 8px;">
                            <h4 style="margin-top: 0;
                                    margin-bottom: 0;
                                    line-height: 1.5;
                                    text-align: left;
                                    font-size: 14px;
                                    font-style: normal;
                                    font-weight: 300;">
                            ${requester}</h4>
                        </td>
                    </tr>
                    <tr>
                        <th width="30%" style="border: 1px solid #ddd;
                                padding: 8px;">
                            <h4 style="
                                                    margin-top: 0;
                                                    margin-bottom: 0;
                                                    line-height: 1.5;
                                                    text-align: left;
                                                    font-size: 14px;
                                                    font-style: normal;">Project Name</h4>
                        </th>
                        <td style="border: 1px solid #ddd;
                            padding: 8px;">
                            <h4 style="margin-top: 0;
                                    margin-bottom: 0;
                                    line-height: 1.5;
                                    text-align: left;
                                    font-size: 14px;
                                    font-style: normal;
                                    font-weight: 300;">${projectname}</h4>
                        </td>
                    </tr>
                    <tr>
                        <th width="30%" style="border: 1px solid #ddd;
                                padding: 8px;">
                            <h4 style="
                                                    margin-top: 0;
                                                    margin-bottom: 0;
                                                    line-height: 1.5;
                                                    text-align: left;
                                                    font-size: 14px;
                                                    font-style: normal;">Cube Name</h4>
                        </th>
                        <td style="border: 1px solid #ddd;
                            padding: 8px;">
                            <h4 style="margin-top: 0;
                                    margin-bottom: 0;
                                    line-height: 1.5;
                                    text-align: left;
                                    font-size: 14px;
                                    font-style: normal;
                                    font-weight: 300;">${cubename}</h4>
                        </td>
                    </tr>
                </table>
            </td>
        </tr>

    </table>
    <hr style="margin-top: 20px;
        margin-bottom: 20px;
        height:0px;
        border-top: 1px solid #eee;
        border-right:0px;
        border-bottom:0px;
        border-left:0px;">
    <h4 style="font-weight: 500;
line-height: 1;font-size: 16px;">

        <p>For any question, please contact support team
            <a href="mailto:DL-eBay-Kylin-Core@ebay.com " style="color: #337ab7; text-decoration: none;">
                <b>DL-eBay-Kylin-DISupport</b>
            </a>
            or Kylin
            <a href="https://ebay-eng.slack.com/messages/C1ZG2TN7J" style="color: #337ab7; text-decoration: none;">
                <b>Slack</b>
            </a> channel.</p>
    </h4>
</div>
</body>

</html>