package org.safehaus.penrose.backend;

import org.safehaus.penrose.ldap.Response;
import org.safehaus.penrose.control.Control;
import org.ietf.ldap.LDAPException;

import java.util.Collection;
import java.util.ArrayList;

/**
 * @author Endi S. Dewata
 */
public class PenroseResponse implements com.identyx.javabackend.Response {

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

    public void addControl(com.identyx.javabackend.Control control) throws Exception {
        PenroseControl penroseControl = (PenroseControl)control;
        response.addControl(penroseControl.getControl());
    }

    public void removeControl(com.identyx.javabackend.Control control) throws Exception {
        PenroseControl penroseControl = (PenroseControl)control;
        response.removeControl(penroseControl.getControl());
    }

    public void setControls(Collection<com.identyx.javabackend.Control> controls) throws Exception {
        Collection<Control> list = new ArrayList<Control>();
        for (com.identyx.javabackend.Control control : controls) {
            PenroseControl penroseControl = (PenroseControl) control;
            list.add(penroseControl.getControl());
        }
        response.setControls(list);
    }

    public Collection<com.identyx.javabackend.Control> getControls() throws Exception {
        Collection<com.identyx.javabackend.Control> list = new ArrayList<com.identyx.javabackend.Control>();
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
