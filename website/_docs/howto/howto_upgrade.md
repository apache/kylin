---
layout: docs
title:  Upgrade From Old Versions
categories: howto
permalink: /docs/howto/howto_upgrade.html
---

Compared with Kylin 3.x and previous versions, Kylin 4.0's storage engine has changed from HBase to Parquet. Therefore, if you need to upgrade from Kylin 3.x and previous versions to kylin4.0, the built cuboid data can't be upgraded, you can only upgrade metadata.

Please refer to : [How to migrate metadata to Kylin 4](https://cwiki.apache.org/confluence/display/KYLIN/How+to+migrate+metadata+to+Kylin+4)

## Upgrade from 4.0.0 to 4.0.1
1) Kylin users can customize the IV value of the encryption algorithm by config `kylin.security.encrypt.cipher.ivSpec` in kylin 4.0.1.
If you uses the default value of `kylin.security.encrypt.cipher.ivSpec`, there is no need to modify the encryption password in kylin.properties.
If you changes the value of `kylin.security.encrypt.cipher.ivSpec`, the encrypted password needs to be re-encrypted.

The encryption algorithm may be used in `kylin.metadata.url(mysql password)`, `kylin.security.ldap.connection-password`, etc.