package org.safehaus.penrose.ldapbackend;

import org.ietf.ldap.LDAPException;

import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public interface Response {

    public void setMessageId(Integer messageId) throws Exception;
    public Integer getMessageId() throws Exception;

    public void addControl(Control control) throws Exception;
    public void removeControl(Control control) throws Exception;
    public void setControls(Collection<Control> controls) throws Exception;
    public Collection<Control> getControls() throws Exception;

    public LDAPException getException();
    public int getReturnCode();
    public String getErrorMessage();
    public String getMessage();
}
