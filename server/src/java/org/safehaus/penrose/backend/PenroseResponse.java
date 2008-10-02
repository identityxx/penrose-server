package org.safehaus.penrose.backend;

import org.safehaus.penrose.ldap.Response;
import org.safehaus.penrose.control.Control;
import org.ietf.ldap.LDAPException;

import java.util.Collection;
import java.util.ArrayList;

/**
 * @author Endi S. Dewata
 */
public class PenroseResponse implements org.safehaus.penrose.ldapbackend.Response {

    Response response;

    public PenroseResponse(Response response) {
        this.response = response;
    }

    public Response getResponse() {
        return response;
    }

    public void setResponse(Response response) {
        this.response = response;
    }

    public void setMessageId(Integer messageId) throws Exception {
        response.setMessageId(messageId);
    }

    public Integer getMessageId() throws Exception  {
        return response.getMessageId();
    }

    public void addControl(org.safehaus.penrose.ldapbackend.Control control) throws Exception {
        PenroseControl penroseControl = (PenroseControl)control;
        response.addControl(penroseControl.getControl());
    }

    public void removeControl(org.safehaus.penrose.ldapbackend.Control control) throws Exception {
        PenroseControl penroseControl = (PenroseControl)control;
        response.removeControl(penroseControl.getControl());
    }

    public void setControls(Collection<org.safehaus.penrose.ldapbackend.Control> controls) throws Exception {
        Collection<Control> list = new ArrayList<Control>();
        for (org.safehaus.penrose.ldapbackend.Control control : controls) {
            PenroseControl penroseControl = (PenroseControl) control;
            list.add(penroseControl.getControl());
        }
        response.setControls(list);
    }

    public Collection<org.safehaus.penrose.ldapbackend.Control> getControls() throws Exception {
        Collection<org.safehaus.penrose.ldapbackend.Control> list = new ArrayList<org.safehaus.penrose.ldapbackend.Control>();
        for (Control control : response.getControls()) {
            list.add(new PenroseControl(control));
        }
        return list;
    }

    public LDAPException getException() {
        return response.getException();
    }

    public int getReturnCode() {
        return response.waitFor();
    }

    public String getErrorMessage() {
        return response.getErrorMessage();
    }

    public String getMessage() {
        return response.getMessage();
    }
}
