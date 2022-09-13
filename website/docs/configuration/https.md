---
title: HTTPS Configuration
language: en
sidebar_label: HTTPS Configuration
pagination_label: HTTPS Configuration
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
    - https configuration
draft: false
last_update:
    date: 08/16/2022
---

Kylin 5.0 provides a HTTPS connection. It is disabled by default. If you need to enable it, please follow the steps below.

### Use Default Certificate

Kylin ships a HTTPS certificate. If you want to enable this function with the default certificate, you just need to add or modify the following properties in `$KYLIN_HOME/conf/kylin.properties`.

```properties
# enable HTTPS connection
kylin.server.https.enable=true
# port number
kylin.server.https.port=7443
```

The default port is `7443`, please check the port has not been taken by other processes. You can run the command below to check. If the port is in use, please use an available port number.

```
netstat -tlpn | grep 7443
```

After modifying the above properties, please restart Kylin for the changes to take effect. Assuming you set the https port to 7443, the access url would be `https://localhost:7443/kylin/index.html`.

**Note:**  Because the certificate is generated automatically, you may see a browser notice about certificate installation when you access the url. Please ignore it.

### User Other Certificates

Kylin also supports third-party certificates, you just need to provide the certificate file and make the following changes in the `$KYLIN_HOME/conf/kylin.properties` file:

```properties
# enable HTTPS connection
kylin.server.https.enable=true
# port number
kylin.server.https.port=7443
# ormat of keystore, Tomcat 8 supports JKS, PKCS11 or PKCS12 format
kylin.server.https.keystore-type=JKS
# location of your certificate file
kylin.server.https.keystore-file=${KYLIN_HOME}/server/.keystore
# password
kylin.server.https.keystore-password=changeit
# alias name for keystore entry, which is optional. Please skip it if you don't need.
kylin.server.https.key-alias=tomcat
```

### Encrypt kylin.server.https.keystore-password
If you need to encrypt `kylin.server.https.keystore-password`, you can do it like this：

i.run following commands in `${KYLIN_HOME}`, it will print encrypted password
```shell
./bin/kylin.sh org.apache.kylin.tool.general.CryptTool -e AES -s <password>
```

ii.config `kylin.server.https.keystore-password` like this
```properties
kylin.server.https.keystore-password=ENC('${encrypted_password}')
```

After modifying the properties above, please restart Kylin for the changes to take effect. Assuming you set the https port to 7443, the access url would be `https://localhost:7443/kylin/index.html`.

> **Note**: If you are not using the default SSL certificate and put your certificate under `$KYLIN_HOME`. Please backup your certificate before upgrading your instance, and specify the file path in the new Kylin configuration file. We recommend putting the certificate under an independent path.
