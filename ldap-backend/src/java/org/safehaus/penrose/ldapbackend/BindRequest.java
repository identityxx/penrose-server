package org.safehaus.penrose.ldapbackend;

/**
 * @author Endi S. Dewata
 */
public interface BindRequest extends Request {

    public void setDn(DN dn) throws Exception;
    public DN getDn() throws Exception;

    public void setPassword(String password) throws Exception;
    public void setPassword(byte[] password) throws Exception;
    public byte[] getPassword() throws Exception;
}
