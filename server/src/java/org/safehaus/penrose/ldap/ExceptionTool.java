package org.safehaus.penrose.ldap;

import org.apache.directory.shared.ldap.message.ResultCodeEnum;
import org.apache.directory.shared.ldap.exception.LdapNamingException;

import javax.naming.NamingException;

/**
 * @author Endi S. Dewata
 */
public class ExceptionTool {

    public static NamingException createNamingException(int rc) {
        return createNamingException(rc, null);
    }

    public static NamingException createNamingException(int rc, String message) {
        ResultCodeEnum rce = ResultCodeEnum.getResultCodeEnum(rc);
        return new LdapNamingException(message, rce);
    }
}
