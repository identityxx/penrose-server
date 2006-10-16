/**
 * Copyright (c) 2000-2006, Identyx Corporation.
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

import javax.naming.*;
import javax.naming.directory.NoSuchAttributeException;
import javax.naming.directory.InvalidAttributeIdentifierException;
import javax.naming.directory.InvalidSearchFilterException;
import javax.naming.directory.AttributeInUseException;

/**
 * @author Endi S. Dewata
 */
public class ExceptionUtil {

    public static int getReturnCode(Throwable t) {

        if (t instanceof CommunicationException) return LDAPException.PROTOCOL_ERROR;
        if (t instanceof TimeLimitExceededException) return LDAPException.TIME_LIMIT_EXCEEDED;
        if (t instanceof SizeLimitExceededException) return LDAPException.SIZE_LIMIT_EXCEEDED;
        if (t instanceof AuthenticationException) return LDAPException.INVALID_CREDENTIALS;
        if (t instanceof NoPermissionException) return LDAPException.INSUFFICIENT_ACCESS_RIGHTS;
        if (t instanceof NoSuchAttributeException) return LDAPException.NO_SUCH_ATTRIBUTE;
        if (t instanceof InvalidAttributeIdentifierException) return LDAPException.UNDEFINED_ATTRIBUTE_TYPE;
        if (t instanceof InvalidSearchFilterException) return LDAPException.INAPPROPRIATE_MATCHING;
        if (t instanceof AttributeInUseException) return LDAPException.ATTRIBUTE_OR_VALUE_EXISTS;
        if (t instanceof NameNotFoundException) return LDAPException.NO_SUCH_OBJECT;
        if (t instanceof NameAlreadyBoundException) return LDAPException.ENTRY_ALREADY_EXISTS;
        if (t instanceof ContextNotEmptyException) return LDAPException.NOT_ALLOWED_ON_NONLEAF;

        return LDAPException.OPERATIONS_ERROR;
    }
}
