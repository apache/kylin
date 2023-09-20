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
<div style="margin-left:5%;margin-right:5%;font-family: 'Trebuchet MS ', Arial, Helvetica, sans-serif;">
<span style="
    line-height: 1.1;font-size: 18px;">
    <p style="text-align:left;">Dear Kylin User,</p>
    <p>We found a job has loaded empty data in your Kylin system as below. </p>
    <p>It won't affect your system stability and you may reload data by following instructions.</p>
    <p>You may refresh the empty segment of the model to reload data.</p>
</span>
    <table cellpadding="0" cellspacing="0" width="100%" style="border-collapse: collapse;border:1px solid #f8f8f8">
        <tr>

            <td style="padding: 15px;">
                <table cellpadding="0" cellspacing="0" width="100%"
                       style="margin-bottom: 20px;border:1 solid #ddd;border-collapse: collapse;font-family: 'Trebuchet MS ', Arial, Helvetica, sans-serif;">
                    <tr>
                        <th width="30%" style="padding: 8px;
                                            line-height: 1.42857143;
                                            vertical-align: top;
                                            border: 1px solid #ddd;
                                            text-align: left;
                                            font-size: medium;
                                            font-style: normal;">Job Type
                        </th>
                        <td style="padding: 8px;
                                line-height: 1.42857143;
                                vertical-align: top;
                                border: 1px solid #ddd;
                                font-size: medium;
                                font-style: normal;">
                            ${job_name}
                        </td>
                    </tr>
                    <tr>
                        <th width="30%" style="padding: 8px;
                                            line-height: 1.42857143;
                                            vertical-align: top;
                                            border: 1px solid #ddd;
                                            text-align: left;
                                            font-size: medium;
                                            font-style: normal;">Submitter
                        </th>
                        <td style="padding: 8px;
                                line-height: 1.42857143;
                                vertical-align: top;
                                border: 1px solid #ddd;
                                font-size: medium;
                                font-style: normal;">
                        ${submitter}
                        </td>
                    </tr>
                    <tr>
                        <th width="30%" style="padding: 8px;
                                            line-height: 1.42857143;
                                            vertical-align: top;
                                            border: 1px solid #ddd;
                                            text-align: left;
                                            font-size: medium;
                                            font-style: normal;">Project
                        </th>
                        <td style="padding: 8px;
                                line-height: 1.42857143;
                                vertical-align: top;
                                border: 1px solid #ddd;
                                font-size: medium;
                                font-style: normal;">
                        ${project_name}
                        </td>
                    </tr>
                    <tr>
                        <th width="30%" style="padding: 8px;
                                            line-height: 1.42857143;
                                            vertical-align: top;
                                            border: 1px solid #ddd;
                                            text-align: left;
                                            font-size: medium;
                                            font-style: normal;">Object
                        </th>
                        <td style="padding: 8px;
                                line-height: 1.42857143;
                                vertical-align: top;
                                border: 1px solid #ddd;
                                font-size: medium;
                                font-style: normal;">
                        ${object}
                        </td>
                    </tr>
                    <tr>
                        <th width="30%" style="padding: 8px;
                                            line-height: 1.42857143;
                                            vertical-align: top;
                                            border: 1px solid #ddd;
                                            text-align: left;
                                            font-size: medium;
                                            font-style: normal;">Start Time
                        </th>
                        <td style="padding: 8px;
                                line-height: 1.42857143;
                                vertical-align: top;
                                border: 1px solid #ddd;
                                font-size: medium;
                                font-style: normal;">
                            ${start_time}
                        </td>
                    </tr>
                    <tr>
                        <th width="30%" style="padding: 8px;
                                            line-height: 1.42857143;
                                            vertical-align: top;
                                            border: 1px solid #ddd;
                                            text-align: left;
                                            font-size: medium;
                                            font-style: normal;">End Time
                        </th>
                        <td style="padding: 8px;
                                line-height: 1.42857143;
                                vertical-align: top;
                                border: 1px solid #ddd;
                                font-size: medium;
                                font-style: normal;">
                            ${end_time}
                        </td>
                    </tr>
                </table>
            </td>
        </tr>
    </table>
    <hr style="margin-top: 20px;
    margin-bottom: 20px;
    border: 0;
    border-top: 1px solid #eee;">
    <h4 style="font-weight: 500;
    line-height: 1.1;font-size:18px;">
        <p>Yours sincerely,</p>
        <p style="margin: 0 0 10px;"><b>Kylin Team</b></p>
    </h4>
</div>
</body>

</html>