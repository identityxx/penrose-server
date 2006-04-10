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


import java.util.Collection;

import org.safehaus.penrose.session.PenroseSession;

/**
 * @author Endi S. Dewata
 */
public class ModifyEvent extends Event {

    public final static int BEFORE_MODIFY = 0;
    public final static int AFTER_MODIFY  = 1;

    private PenroseSession session;
    private String dn;
    private int returnCode;

    private Collection modifications;

    public ModifyEvent(Object source, int type, PenroseSession session, String dn, Collection modifications) {
        super(source, type);
        this.session = session;
        this.dn = dn;
        this.modifications = modifications;
    }

    public String getDn() {
        return dn;
    }

    public void setDn(String dn) {
        this.dn = dn;
    }

    public Collection getModifications() {
        return modifications;
    }

    public void setModifications(Collection modifications) {
        this.modifications = modifications;
    }

    public int getReturnCode() {
        return returnCode;
    }

    public void setReturnCode(int returnCode) {
        this.returnCode = returnCode;
    }

    public PenroseSession getConnection() {
        return session;
    }

    public void setConnection(PenroseSession session) {
        this.session = session;
    }
}
