package org.apache.kylin.rest.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.ldap.userdetails.LdapUserDetailsService;
import org.springframework.security.saml.SAMLCredential;

/**
 * An implementation of SAMLUserDetailsService by delegating the query to LdapUserDetailsService.
 */
public class SAMLUserDetailsService implements org.springframework.security.saml.userdetails.SAMLUserDetailsService {

    private static final Logger logger = LoggerFactory.getLogger(SAMLUserDetailsService.class);
    private LdapUserDetailsService ldapUserDetailsService;

    public SAMLUserDetailsService(LdapUserDetailsService ldapUserDetailsService) {
        this.ldapUserDetailsService = ldapUserDetailsService;
    }

    @Override
    public Object loadUserBySAML(SAMLCredential samlCredential) throws UsernameNotFoundException {
        final String userEmail = samlCredential.getAttributeAsString("email");
        logger.debug("samlCredential.email:" + userEmail);
        final String userName = userEmail.substring(0, userEmail.indexOf("@"));

        
        UserDetails userDetails = null;
        try {
            userDetails = ldapUserDetailsService.loadUserByUsername(userName);
        } catch (org.springframework.security.core.userdetails.UsernameNotFoundException e) {
            logger.error("User not found in LDAP, check whether he/she has been added to the groups.", e);
        }
        logger.debug("userDeail by search ldap with '" + userName + "' is: " + userDetails);
        return userDetails;
    }
}
