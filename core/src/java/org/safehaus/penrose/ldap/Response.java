package org.safehaus.penrose.ldap;

import org.safehaus.penrose.control.Control;
import org.ietf.ldap.LDAPException;

import java.util.Collection;
import java.util.ArrayList;
import java.io.Serializable;

/**
 * @author Endi S. Dewata
 */
public class Response implements Serializable, Cloneable {

    protected Collection<Control> controls  = new ArrayList<Control>();

    protected LDAPException exception = LDAP.createException(LDAP.SUCCESS);

    public void addControl(Control control) {
        controls.add(control);
    }

    public void setControls(Collection<Control> controls) {
        this.controls.clear();
        if (controls != null) this.controls.addAll(controls);
    }

    public void removeControl(Control control) {
        controls.remove(control);
    }

    public Collection<Control> getControls() {
        return controls;
    }

    public int waitFor() {
        return exception.getResultCode();
    }

    public void setException(LDAPException e) {
        this.exception = e;
    }

    public void setException(Exception e) {
        exception = LDAP.createException(e);
    }

    public LDAPException getException() {
        return exception;
    }

    public void setReturnCode(int returnCode) {
        exception = LDAP.createException(returnCode);
    }

    public int getReturnCode() {
        return exception.getResultCode();
    }

    public String getErrorMessage() {
        return exception.getLDAPErrorMessage();
    }

    public String getMessage() {
        return exception.getMessage();
    }

    public void copy(Response response) {
        controls = new ArrayList<Control>();
        controls.addAll(response.controls);

        exception = response.exception;
    }

    public Object clone() throws CloneNotSupportedException {
        Response response = (Response)super.clone();
        response.copy(this);
        return response;
    }
}
