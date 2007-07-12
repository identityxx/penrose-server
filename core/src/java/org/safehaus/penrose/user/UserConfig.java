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
package org.safehaus.penrose.user;

import org.safehaus.penrose.ldap.DN;

import java.util.Arrays;

/**
 * @author Endi S. Dewata
 */
public class UserConfig implements Cloneable {

    private DN dn;
    private byte[] password;

    public UserConfig() {
    }

    public UserConfig(String dn, String password) {
        this.dn = new DN(dn);
        this.password = password.getBytes();
    }

    public UserConfig(DN dn, String password) {
        this.dn = dn;
        this.password = password.getBytes();
    }

    public DN getDn() {
        return dn;
    }

    public void setDn(String dn) {
        this.dn = new DN(dn);
    }

    public void setDn(DN dn) {
        this.dn = dn;
    }

    public byte[] getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password == null ? null : password.getBytes();
    }

    public void setPassword(byte[] password) {
        this.password = password;
    }

    public int hashCode() {
        return (dn == null ? 0 : dn.hashCode()) +
                (password == null ? 0 : password.hashCode());
    }

    boolean equals(Object o1, Object o2) {
        if (o1 == null && o2 == null) return true;
        if (o1 != null) return o1.equals(o2);
        return o2.equals(o1);
    }

    public boolean equals(Object object) {
        if((object == null) || (object.getClass() != getClass())) return false;

        UserConfig userConfig = (UserConfig)object;
        if (!equals(dn, userConfig.dn)) return false;
        if (!Arrays.equals(password, userConfig.password)) return false;

        return true;
    }

    public void copy(UserConfig userConfig) {
        dn = userConfig.dn;
        password = userConfig.password.clone();
    }

    public Object clone() throws CloneNotSupportedException {
        UserConfig userConfig = (UserConfig)super.clone();
        userConfig.copy(this);
        return userConfig;
    }
}
