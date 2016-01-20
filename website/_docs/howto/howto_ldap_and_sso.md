---
layout: docs
title:  How to Enable Security with LDAP and SSO
categories: howto
permalink: /docs/howto/howto_ldap_and_sso.html
version: v2.0
since: v1.0
---

## Enable LDAP authentication

Kylin supports LDAP authentication for enterprise or production deployment; This is implemented with Spring Security framework; Before enable LDAP, please contact your LDAP administrator to get necessary information, like LDAP server URL, username/password, search patterns;

#### Configure LDAP server info

Firstly, provide LDAP URL, and username/password if the LDAP server is secured;

```
ldap.server=ldap://<your_ldap_host>:<port>
ldap.username=<your_user_name>
ldap.password=<your_password>
```

Secondly, provide the user search patterns, this is by LDAP design, here is just a sample:

```
ldap.user.searchBase=OU=UserAccounts,DC=mycompany,DC=com
ldap.user.searchPattern=(&(AccountName={0})(memberOf=CN=MYCOMPANY-USERS,DC=mycompany,DC=com))
ldap.user.groupSearchBase=OU=Group,DC=mycompany,DC=com
```

If you have service accounts (e.g, for system integration) which also need be authenticated, configure them in ldap.service.*; Otherwise, leave them be empty;

### Configure the administrator group and default role

To map an LDAP group to the admin group in Kylin, need set the "acl.adminRole" to "ROLE_" + GROUP_NAME. For example, in LDAP the group "KYLIN-ADMIN-GROUP" is the list of administrators, here need set it as:

```
acl.adminRole=ROLE_KYLIN-ADMIN-GROUP
acl.defaultRole=ROLE_ANALYST,ROLE_MODELER
```

The "acl.defaultRole" is a list of the default roles that grant to everyone, keep it as-is.

#### Enable LDAP

For Kylin v0.x and v1.x: set "kylin.sandbox=false" in conf/kylin.properties, then restart Kylin server; 
For Kylin since v2.0: set "kylin.security.profile=ldap" in conf/kylin.properties, then restart Kylin server; 

## Enable SSO authentication

From v2.0, Kylin provides SSO with SAML. The implementation is based on Spring Security SAML Extension. You can read [this reference](http://docs.spring.io/autorepo/docs/spring-security-saml/1.0.x-SNAPSHOT/reference/htmlsingle/) to get an overall understand.

Before trying this, you should have successfully enabled LDAP and managed users with it, as SSO server may only do authentication, Kylin need search LDAP to get the user's detail information.

### Generate IDP metadata xml
Contact your IDP (ID provider), asking to generate the SSO metadata file; Usually you need provide three piece of info:

  1. Partner entity ID, which is an unique ID of your app, e.g,: https://host-name/kylin/saml/metadata 
  2. App callback endpoint, to which the SAML assertion be posted, it need be: https://host-name/kylin/saml/SSO
  3. Public certificate of Kylin server, the SSO server will encrypt the message with it.

### Generate JKS keystore for Kylin
As Kylin need send encrypted message (signed with Kylin's private key) to SSO server, a keystore (JKS) need be provided. There are a couple ways to generate the keystore, below is a sample.

Assume kylin.crt is the public certificate file, kylin.key is the private certificate file; firstly create a PKCS#12 file with openssl, then convert it to JKS with keytool: 

```
$ openssl pkcs12 -export -in kylin.crt -inkey kylin.key -out kylin.p12
Enter Export Password: <export_pwd>
Verifying - Enter Export Password: <export_pwd>


$ keytool -importkeystore -srckeystore kylin.p12 -srcstoretype PKCS12 -srcstorepass <export_pwd> -alias 1 -destkeystore samlKeystore.jks -destalias kylin -destkeypass changeit

Enter destination keystore password:  changeit
Re-enter new password: changeit

```

It will put the keys to "samlKeystore.jks" with alias "kylin";

### Enable Higher Ciphers

Make sure your environment is ready to handle higher level crypto keys, you may need to download Java Cryptography Extension (JCE) Unlimited Strength Jurisdiction Policy Files, copy local_policy.jar and US_export_policy.jar to $JAVA_HOME/jre/lib/security .

### Deploy IDP xml file and keystore to Kylin

The IDP metadata and keystore file need be deployed in Kylin web app's classpath in $KYLIN_HOME/tomcat/webapps/kylin/WEB-INF/classes 
	
  1. Name the IDP file to sso_metadata.xml and then copy to Kylin's classpath;
  2. Name the keystore as "samlKeystore.jks" and then copy to Kylin's classpath;
  3. If you use another alias or password, remember to update that kylinSecurity.xml accordingly:

```
<!-- Central storage of cryptographic keys -->
<bean id="keyManager" class="org.springframework.security.saml.key.JKSKeyManager">
	<constructor-arg value="classpath:samlKeystore.jks"/>
	<constructor-arg type="java.lang.String" value="changeit"/>
	<constructor-arg>
		<map>
			<entry key="kylin" value="changeit"/>
		</map>
	</constructor-arg>
	<constructor-arg type="java.lang.String" value="kylin"/>
</bean>

```

### Other configurations
In conf/kylin.properties, add the following properties with your server information:

```
saml.metadata.entityBaseURL=https://host-name/kylin
saml.context.scheme=https
saml.context.serverName=host-name
saml.context.serverPort=443
saml.context.contextPath=/kylin

```

Please note, Kylin assume in the SAML message there is a "email" attribute representing the login user, and the name before @ will be used to search LDAP. 

### Enable SSO
Set "kylin.security.profile=saml" in conf/kylin.properties, then restart Kylin server; After enabling SSO, when you try to access a URL like "/kylin" or "/kylin/cubes", it will redirect to SSO for login, and jump back after authorized. While login with LDAP is still available, you can type "/kylin/login" to use original way. The Rest API (/kylin/api/*) still use LDAP + Basic authentication, no impact.

