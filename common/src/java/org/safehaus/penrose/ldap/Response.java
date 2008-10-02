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

    protected Integer messageId;
    protected Collection<Control> controls  = new ArrayList<Control>();

    protected LDAPException exception = LDAP.createException(LDAP.SUCCESS);

    public Response() {
    }

    public void setMessageId(Integer messageId) {
        this.messageId = messageId;
    }

    public Integer getMessageId() {
        return messageId;
    }

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

    public int hashCode() {
        return controls.hashCode();
    }

    private boolean equals(Object o1, Object o2) {
        if (o1 == null && o2 == null) return true;
        if (o1 != null) return o1.equals(o2);
        return o2.equals(o1);
    }

    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null) return false;
        if (object.getClass() != this.getClass()) return false;

        Response response = (Response)object;
        if (!equals(messageId, response.messageId)) return false;
        if (!equals(controls, response.controls)) return false;
        if (!equals(exception, response.exception)) return false;

        return true;
    }

    public void copy(Response response) {

        messageId = response.messageId;

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
