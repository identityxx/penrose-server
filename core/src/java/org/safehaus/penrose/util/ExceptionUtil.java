/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.safehaus.penrose.util;

import org.ietf.ldap.LDAPException;
import org.apache.directory.shared.ldap.exception.LdapNamingException;
import org.apache.directory.shared.ldap.message.ResultCodeEnum;

import javax.naming.NamingException;

/**
 * @author Endi S. Dewata
 */
public class ExceptionUtil {

    public static int getReturnCode(Throwable t) {
        ResultCodeEnum rc = ResultCodeEnum.getResultCode(t);
        if (rc == null) return LDAPException.OPERATIONS_ERROR;
        return rc.getValue();
    }

    public static void throwNamingException(int rc) throws NamingException {
        throwNamingException(rc, null);
    }

    public static void throwNamingException(int rc, String message) throws NamingException {
        ResultCodeEnum rce = ResultCodeEnum.getResultCodeEnum(rc);
        throw new LdapNamingException(message, rce);
    }
}
