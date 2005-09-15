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
package org.safehaus.penrose.event;

import org.safehaus.penrose.PenroseConnection;
import org.ietf.ldap.LDAPEntry;

/**
 * @author Endi S. Dewata
 */
public class AddEvent extends Event {

    public final static int BEFORE_ADD = 0;
    public final static int AFTER_ADD  = 1;

    private PenroseConnection connection;
    private LDAPEntry entry;
    private int returnCode;

    public AddEvent(Object source, int type, PenroseConnection connection, LDAPEntry entry) {
        super(source, type);
        this.connection = connection;
        this.entry = entry;
    }

    public int getReturnCode() {
        return returnCode;
    }

    public void setReturnCode(int returnCode) {
        this.returnCode = returnCode;
    }

    public LDAPEntry getEntry() {
        return entry;
    }

    public void setEntry(LDAPEntry entry) {
        this.entry = entry;
    }

    public PenroseConnection getConnection() {
        return connection;
    }

    public void setConnection(PenroseConnection connection) {
        this.connection = connection;
    }
}
