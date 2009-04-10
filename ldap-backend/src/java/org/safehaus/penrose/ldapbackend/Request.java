package org.safehaus.penrose.ldapbackend;

import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public interface Request {

    public void setMessageId(Integer messageId) throws Exception;
    public Integer getMessageId() throws Exception;

    public void addControl(Control control) throws Exception;
    public void removeControl(Control control) throws Exception;
    public void setControls(Collection<Control> controls) throws Exception;
    public Collection<Control> getControls() throws Exception;
}
