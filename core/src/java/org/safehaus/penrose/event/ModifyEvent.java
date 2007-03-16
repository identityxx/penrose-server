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

import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.session.ModifyRequest;
import org.safehaus.penrose.session.ModifyResponse;

/**
 * @author Endi S. Dewata
 */
public class ModifyEvent extends Event {

    public final static int BEFORE_MODIFY = 0;
    public final static int AFTER_MODIFY  = 1;

    protected Session session;
    protected int returnCode;

    protected ModifyRequest request;
    protected ModifyResponse response;

    public ModifyEvent(Object source, int type, Session session, ModifyRequest request, ModifyResponse response) {
        super(source, type);

        this.session = session;
        this.request = request;
        this.response = response;
    }

    public int getReturnCode() {
        return returnCode;
    }

    public void setReturnCode(int returnCode) {
        this.returnCode = returnCode;
    }

    public Session getSession() {
        return session;
    }

    public void setSession(Session session) {
        this.session = session;
    }

    public ModifyRequest getRequest() {
        return request;
    }

    public void setRequest(ModifyRequest request) {
        this.request = request;
    }

    public ModifyResponse getResponse() {
        return response;
    }

    public void setResponse(ModifyResponse response) {
        this.response = response;
    }

    public String toString() {
        return (type == BEFORE_MODIFY ? "Before" : "After")+"Modify";
    }
}
