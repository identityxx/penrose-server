package org.safehaus.penrose.ldapbackend;

/**
 * @author Endi S. Dewata
 */
public interface AddRequest extends Request {

    public void setDn(DN dn) throws Exception;
    public DN getDn() throws Exception;

    public void setAttributes(Attributes attributes) throws Exception;
    public Attributes getAttributes() throws Exception;
}
