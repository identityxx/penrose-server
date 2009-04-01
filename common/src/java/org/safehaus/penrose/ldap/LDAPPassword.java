/**
 * Copyright 2009 Red Hat, Inc.
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
package org.safehaus.penrose.ldap;

import org.safehaus.penrose.password.Password;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/**
 * @author Endi S. Dewata
 */
public class LDAPPassword {

    public static Logger log = LoggerFactory.getLogger(LDAPPassword.class);

    public static boolean validate(String password, String digest) throws Exception {

        if (digest == null) return false;

        String encryption = getEncryptionMethod(digest);
        String hash       = getEncryptedPassword(digest);

        return Password.validate(encryption, password, hash);
    }

    public static String getEncryptionMethod(String password) {

        if (password == null) return null;
        if (!password.startsWith("{")) return null;

        int i = password.indexOf("}");
        if (i < 0) return null;

        return password.substring(1, i);
    }

    public static String getEncryptedPassword(String password) {

        if (password == null) return null;
        if (!password.startsWith("{")) return password;

        int i = password.indexOf("}");
        if (i < 0) return password;

        return password.substring(i+1);
    }
}
