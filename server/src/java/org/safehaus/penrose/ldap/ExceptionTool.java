package org.safehaus.penrose.ldap;

import org.apache.directory.shared.ldap.message.ResultCodeEnum;
import org.apache.directory.shared.ldap.exception.LdapNamingException;

import javax.naming.NamingException;

/**
 * @author Endi S. Dewata
 */
public class ExceptionTool {
    
    public static void throwNamingException(int rc) throws NamingException {
        throwNamingException(rc, null);
    }

    public static void throwNamingException(int rc, String message) throws NamingException {
        ResultCodeEnum rce = ResultCodeEnum.getResultCodeEnum(rc);
        throw new LdapNamingException(message, rce);
    }
}
