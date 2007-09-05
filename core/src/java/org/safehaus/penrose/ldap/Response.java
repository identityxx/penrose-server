package org.safehaus.penrose.ldap;

import org.safehaus.penrose.control.Control;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ietf.ldap.LDAPException;

import java.util.Collection;
import java.util.ArrayList;
import java.io.Serializable;

/**
 * @author Endi S. Dewata
 */
public class Response implements Serializable {

    public Logger log = LoggerFactory.getLogger(getClass());
    public Logger errorLog = org.safehaus.penrose.log.Error.log;
    public boolean debug = log.isDebugEnabled();

    protected Collection<Control> controls  = new ArrayList<Control>();

    protected LDAPException exception = LDAP.createException(LDAP.SUCCESS);

    public void addControl(Control control) {
        controls.add(control);
    }

    public void removeControl(Control control) {
        controls.remove(control);
    }

    public void setControls(Collection<Control> controls) {
        this.controls.clear();
        if (controls != null) this.controls.addAll(controls);
    }

    public Collection<Control> getControls() {
        return controls;
    }

    public LDAPException getException() {
        return exception;
    }

    public void setException(LDAPException exception) {
        this.exception = exception;
    }

    public void setException(Exception e) {
        exception = LDAP.createException(e);
    }

    public int getReturnCode() {
        return exception.getResultCode();
    }

    public void setReturnCode(int returnCode) {
        exception = LDAP.createException(returnCode);
    }

    public String getMessage() {
        return exception.getLDAPErrorMessage();
    }
}
