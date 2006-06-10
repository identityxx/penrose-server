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

import org.safehaus.penrose.session.PenroseSession;

/**
 * @author Endi S. Dewata
 */
public class SearchEvent extends Event {

    public final static int BEFORE_SEARCH = 0;
    public final static int AFTER_SEARCH  = 1;

    private PenroseSession session;
    private String base;
    private String filter;
    private int returnCode;

    public SearchEvent(Object source, int type, PenroseSession session, String base, String filter) {
        super(source, type);
        this.session = session;
        this.base = base;
        this.filter = filter;
    }

    public PenroseSession getSession() {
        return session;
    }

    public void setSession(PenroseSession session) {
        this.session = session;
    }

    public String getBase() {
        return base;
    }

    public void setBase(String base) {
        this.base = base;
    }

    public int getReturnCode() {
        return returnCode;
    }

    public void setReturnCode(int returnCode) {
        this.returnCode = returnCode;
    }

    public String getFilter() {
        return filter;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }
}
