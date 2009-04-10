package org.safehaus.penrose.ldapbackend;

/**
 * @author Endi S. Dewata
 */
public interface DeleteRequest extends Request {

    public void setDn(DN dn) throws Exception;
    public DN getDn() throws Exception;
}
