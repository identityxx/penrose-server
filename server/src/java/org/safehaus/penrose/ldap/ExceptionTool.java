package org.safehaus.penrose.ldap;

import org.apache.directory.shared.ldap.message.ResultCodeEnum;
import org.apache.directory.shared.ldap.exception.LdapNamingException;
import org.ietf.ldap.LDAPException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.NamingException;

/**
 * @author Endi S. Dewata
 */
public class ExceptionTool {
    
    public static Logger log = LoggerFactory.getLogger(ExceptionTool.class);

    public static NamingException createNamingException(int rc) {
        return createNamingException(rc, null);
    }

    public static NamingException createNamingException(LDAPException e) {
        return createNamingException(e.getResultCode(), e.getLDAPErrorMessage());
    }

    public static NamingException createNamingException(int rc, String message) {
        ResultCodeEnum rce = ResultCodeEnum.getResultCodeEnum(rc);
        log.debug("RC: "+rc+" - "+message);
        return new LdapNamingException(message, rce);
    }
}
