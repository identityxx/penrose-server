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
package org.safehaus.penrose.event;

import org.safehaus.penrose.session.PenroseSession;
import org.safehaus.penrose.entry.DN;

/**
 * @author Endi S. Dewata
 */
public class BindEvent extends Event {

    public final static int BEFORE_BIND = 0;
    public final static int AFTER_BIND  = 1;

    public final static int BEFORE_UNBIND = 3;
    public final static int AFTER_UNBIND  = 4;

    private PenroseSession session;
    private DN dn;
    private int returnCode;

    private String password;

    public BindEvent(Object source, int type, PenroseSession session, DN dn, String password) {
        super(source, type);
        this.session = session;
        this.dn = dn;
        this.password = password;
    }

    public BindEvent(Object source, int type, PenroseSession session, DN dn) {
        super(source, type);
        this.session = session;
        this.dn = dn;
    }

    public int getReturnCode() {
        return returnCode;
    }

    public void setReturnCode(int returnCode) {
        this.returnCode = returnCode;
    }

    public DN getDn() {
        return dn;
    }

    public void setDn(DN dn) {
        this.dn = dn;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public PenroseSession getSession() {
        return session;
    }

    public void setSession(PenroseSession session) {
        this.session = session;
    }
}
