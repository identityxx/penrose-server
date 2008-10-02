package org.safehaus.penrose.backend;

import org.safehaus.penrose.ldap.Request;
import org.safehaus.penrose.control.Control;

import java.util.Collection;
import java.util.ArrayList;

/**
 * @author Endi S. Dewata
 */
public class PenroseRequest implements org.safehaus.penrose.ldapbackend.Request {

    Request request;

    public PenroseRequest(Request request) {
        this.request = request;
    }

    public Request getRequest() {
        return request;
    }

    public void setRequest(Request request) {
        this.request = request;
    }

    public void setMessageId(Integer messageId) throws Exception {
        request.setMessageId(messageId);
    }

    public Integer getMessageId() throws Exception  {
        return request.getMessageId();
    }

    public void addControl(org.safehaus.penrose.ldapbackend.Control control) throws Exception {
        PenroseControl penroseControl = (PenroseControl)control;
        request.addControl(penroseControl.getControl());
    }

    public void removeControl(org.safehaus.penrose.ldapbackend.Control control) throws Exception {
        PenroseControl penroseControl = (PenroseControl)control;
        request.removeControl(penroseControl.getControl());
    }

    public void setControls(Collection<org.safehaus.penrose.ldapbackend.Control> controls) throws Exception {
        Collection<Control> list = new ArrayList<Control>();
        for (org.safehaus.penrose.ldapbackend.Control control : controls) {
            PenroseControl penroseControl = (PenroseControl)control;
            list.add(penroseControl.getControl());
        }
        request.setControls(list);
    }

    public Collection<org.safehaus.penrose.ldapbackend.Control> getControls() throws Exception {
        Collection<org.safehaus.penrose.ldapbackend.Control> list = new ArrayList<org.safehaus.penrose.ldapbackend.Control>();
        for (Control control : request.getControls()) {
            list.add(new PenroseControl(control));
        }
        return list;
    }
}
