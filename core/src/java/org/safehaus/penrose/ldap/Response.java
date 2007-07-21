package org.safehaus.penrose.ldap;

import org.safehaus.penrose.control.Control;
import org.safehaus.penrose.util.ExceptionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ietf.ldap.LDAPException;

import java.util.Collection;
import java.util.ArrayList;

/**
 * @author Endi S. Dewata
 */
public class Response {

    public Logger log = LoggerFactory.getLogger(getClass());
    public boolean debug = log.isDebugEnabled();

    protected Collection<Control> controls   = new ArrayList<Control>();

    protected LDAPException exception = ExceptionUtil.createLDAPException(0);

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
        exception = ExceptionUtil.createLDAPException(e);
    }

    public int getReturnCode() {
        return exception.getResultCode();
    }

    public void setReturnCode(int returnCode) {
        exception = ExceptionUtil.createLDAPException(returnCode);
    }

    public String getMessage() {
        return exception.getLDAPErrorMessage();
    }
}
