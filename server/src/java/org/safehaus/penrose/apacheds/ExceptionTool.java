package org.safehaus.penrose.apacheds;

import org.apache.directory.shared.ldap.message.ResultCodeEnum;
import org.apache.directory.shared.ldap.exception.LdapNamingException;
import org.ietf.ldap.LDAPException;

import javax.naming.NamingException;

/**
 * @author Endi S. Dewata
 */
public class ExceptionTool {

    public static NamingException createNamingException(Exception e) {
        if (e instanceof NamingException) return (NamingException)e;

        if (e instanceof LDAPException) {
            LDAPException ldapException = (LDAPException)e;
            return createNamingException(ldapException.getResultCode(), ldapException.getMessage());
        }
        
        ResultCodeEnum rce = ResultCodeEnum.getResultCode(e);
        return new LdapNamingException(e.getMessage(), rce);
    }

    public static NamingException createNamingException(int rc) {
        return createNamingException(rc, null);
    }

    public static NamingException createNamingException(int rc, String message) {
        ResultCodeEnum rce = ResultCodeEnum.getResultCodeEnum(rc);
        return new LdapNamingException(message, rce);
    }
}
