/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 * All rights reserved.
 */
package org.safehaus.penrose.util;

import org.ietf.ldap.LDAPException;

import javax.naming.NamingException;
import javax.naming.NoPermissionException;
import javax.naming.NameNotFoundException;
import javax.naming.NameAlreadyBoundException;

/**
 * @author Endi S. Dewata
 */
public class ExceptionUtil {

    public static void throwNamingException(int rc) throws NamingException {
        throwNamingException(rc, null);
    }

    public static void throwNamingException(int rc, String message) throws NamingException {
        switch (rc) {
            case LDAPException.ENTRY_ALREADY_EXISTS:
                throw new NameAlreadyBoundException(message);

            case LDAPException.NO_SUCH_OBJECT:
                throw new NameNotFoundException(message);

            case LDAPException.INSUFFICIENT_ACCESS_RIGHTS:
                throw new NoPermissionException(message);

            default:
                throw new NamingException(message);
        }
    }
}
